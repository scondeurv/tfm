//! Distributed Single-Source Shortest Path (SSSP) action.
//!
//! # Overview
//!
//! Iterative relaxation (Bellman-Ford style) over a weighted directed graph
//! with non-negative edge weights. Each Burst worker:
//!
//! 1. Fetches its assigned graph partition from S3.
//! 2. Builds a local Compressed-Sparse-Row (CSR) representation.
//! 3. At every iteration relaxes the out-edges of nodes it owns,
//!    starting from the previous global distance vector.
//! 4. Synchronises with peers via `reduce` + `broadcast`. The reduce
//!    operator is element-wise minimum on the `u32` bit patterns of the
//!    distances — for non-negative finite `f32` (and `+∞`) this is
//!    equivalent to `f32::min` without decoding.
//! 5. Stops when no worker improved any distance, or when
//!    `max_iterations` is reached.
//!
//! Worker `0` (the `ROOT_WORKER`) writes the final per-node distance
//! vector to S3 and emits a human-readable summary.
//!
//! # Partition model
//!
//! - Exactly one global partition per worker (`partitions == burst_size`).
//! - Workers may have overlapping ownership of nodes; the reduce step
//!   resolves conflicts deterministically by taking the smallest distance.
//!
//! # Wire format
//!
//! Distances travel as [`DistanceMessage`] — a `Vec<u32>` of length
//! `num_nodes + 1`. The first `num_nodes` slots carry the f32 bit patterns
//! of the per-node distances; the trailing slot piggybacks the local
//! relaxation count for cheap convergence checks. Conversion to/from
//! [`Bytes`] uses explicit little-endian encoding.
//!
//! # Within-worker semantics
//!
//! Within a single iteration, each relaxation reads `dist[u]` from the
//! previous global vector and writes improvements into a local candidate
//! buffer. This keeps the algorithm synchronous across workers and aligned
//! with the standalone Bellman-Ford baseline.

use std::{
    fmt::Write as _,
    time::{SystemTime, UNIX_EPOCH},
};

use aws_config::Region;
use aws_credential_types::Credentials;
use aws_sdk_s3::Client as S3Client;
use burst_communication_middleware::{Middleware, MiddlewareActorHandle};
use bytes::Bytes;
use serde_derive::{Deserialize, Serialize};
use serde_json::{Error, Value};

/// Worker that hosts the canonical aggregation role: roots the
/// reduce/broadcast collectives and writes the final S3 output.
const ROOT_WORKER: u32 = 0;

/// Default cap on relaxation iterations when the input does not specify one.
const MAX_ITERATIONS: u32 = 500;

/// `f32::INFINITY.to_bits()`. Sentinel for "node not yet reached".
const INF_BITS: u32 = 0x7F80_0000;

/// JSON-serialisable input the action receives from the Burst harness.
///
/// Field invariants:
/// - `partitions` must equal `burst_size` (one partition per worker).
/// - `partitions % granularity == 0` for balanced placement.
/// - `source_node` defaults to `0`; `max_iterations` defaults to
///   [`MAX_ITERATIONS`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Input {
    /// S3 location and credentials for the partition shards.
    input_data: S3InputParams,
    /// Total number of vertices in the global graph.
    num_nodes: u32,
    /// SSSP source vertex. Defaults to `0` if absent.
    source_node: Option<u32>,
    /// Cap on relaxation iterations. Defaults to [`MAX_ITERATIONS`].
    max_iterations: Option<u32>,
    /// Number of partitions the graph was sharded into. Equals `burst_size`.
    partitions: u32,
    /// Workers per Burst pack/group. `partitions % granularity == 0`.
    granularity: u32,
    /// Optional group id (multi-pack scheduling). Accepted for forward
    /// compatibility but not consumed by the action itself.
    #[serde(default)]
    group_id: Option<u32>,
    /// Reserved for a future per-call collective-operation timeout
    /// override; currently parsed but not applied.
    timeout_seconds: Option<u64>,
}

/// S3 endpoint + credentials + key prefix where partition shards live.
///
/// Each shard is fetched from `{bucket}/{key}/part-{worker_id:05}`. When
/// `endpoint` is set (non-AWS S3-compatible stores like MinIO),
/// path-style addressing is forced.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct S3InputParams {
    bucket: String,
    key: String,
    region: String,
    endpoint: Option<String>,
    aws_access_key_id: String,
    aws_secret_access_key: String,
    aws_session_token: Option<String>,
}

/// JSON-serialisable result returned to the Burst harness, one per worker.
///
/// Only [`ROOT_WORKER`] populates `results`; non-root workers leave it as
/// `None` and contribute only their `timestamps` for performance analysis.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Output {
    bucket: String,
    key: String,
    /// Performance trace, used by `runtime_metrics.py` to compute phase timings.
    timestamps: Vec<Timestamp>,
    /// Human-readable summary written by [`ROOT_WORKER`] only.
    #[serde(skip_serializing_if = "Option::is_none")]
    results: Option<String>,
}

/// One performance-trace event: `key` is the phase name (e.g. `iter_3_compute`)
/// and `value` is the millisecond Unix timestamp captured when it occurred.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Timestamp {
    key: String,
    value: String,
}

/// Capture the current wall-clock time as a [`Timestamp`] under `key`.
///
/// # Panics
/// If the system clock predates the Unix epoch.
fn timestamp(key: &str) -> Timestamp {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis();
    Timestamp {
        key: key.to_string(),
        value: now.to_string(),
    }
}

/// Distance vector exchanged between workers each iteration.
///
/// Layout: a `Vec<u32>` of length `num_nodes + 1`. The first `num_nodes`
/// slots carry the bit patterns of the per-node distances (so an
/// unreached node is `INF_BITS`), and the trailing slot piggybacks the
/// local relaxation count.
///
/// # Bit-level minimum trick
///
/// For non-negative `f32` plus `+∞`, the lexicographic order of `u32`
/// bit patterns matches the order of the float values. Therefore an
/// element-wise `u32::min` on two such vectors is equivalent to an
/// element-wise `f32::min` — no decode/encode is required during the
/// reduce hot path.
///
/// The [`Bytes`] conversions use explicit little-endian encoding to avoid
/// allocator-layout assumptions while keeping the wire format stable.
#[derive(Debug, Clone, PartialEq)]
pub struct DistanceMessage(pub Vec<u32>);

impl From<Bytes> for DistanceMessage {
    /// Decode a little-endian byte buffer into `Vec<u32>`.
    ///
    /// # Panics
    /// If `bytes.len()` is not a multiple of `4`.
    fn from(bytes: Bytes) -> Self {
        if bytes.is_empty() {
            return DistanceMessage(vec![]);
        }
        assert!(
            bytes.len() % 4 == 0,
            "DistanceMessage byte length {} not divisible by 4",
            bytes.len()
        );
        DistanceMessage(
            bytes
                .chunks_exact(4)
                .map(|chunk| u32::from_le_bytes(chunk.try_into().unwrap()))
                .collect(),
        )
    }
}

impl From<DistanceMessage> for Bytes {
    /// Encode `Vec<u32>` into little-endian bytes.
    fn from(val: DistanceMessage) -> Self {
        let mut bytes = Vec::with_capacity(val.0.len() * 4);
        for value in val.0 {
            bytes.extend_from_slice(&value.to_le_bytes());
        }
        Bytes::from(bytes)
    }
}

/// Compressed-Sparse-Row representation of the per-worker subgraph
/// (with edge weights).
///
/// - `owned_nodes` lists the source nodes the worker actually has
///   out-edges for; nodes absent from this list have an empty range in
///   `offsets` and contribute nothing to the relaxation.
/// - `offsets[i]` is the start of node `i`'s adjacency list,
///   `offsets[i+1]` the end. `offsets` has length `num_nodes + 1`.
/// - `flat_neighbors` and `flat_weights` are parallel arrays containing
///   the destination ids and the corresponding edge weights.
struct CSRGraph {
    owned_nodes: Vec<u32>,
    offsets: Vec<u32>,
    flat_neighbors: Vec<u32>,
    flat_weights: Vec<f32>,
}

/// Parse one partition's tab-separated body into a weighted edge list.
///
/// Each non-blank line has the form `src \t dst [\t weight]`. The weight
/// column is optional and defaults to `1.0` when omitted. Lines with an
/// unparseable `src`/`dst`, or whose endpoints fall outside
/// `[0, num_nodes)`, are skipped with a warning written to `stderr`.
///
/// # Panics
/// If any parsed weight is not strictly non-negative (negative or NaN).
/// Bellman-Ford with negative weights or NaN propagation is unsupported
/// by this benchmark.
fn parse_partition_body(
    worker_id: u32,
    part_key: &str,
    body: &str,
    num_nodes: u32,
    edges: &mut Vec<(u32, u32, f32)>,
) {
    for line in body.lines() {
        if line.trim().is_empty() {
            continue;
        }
        let mut it = line.split('\t');
        let src = it.next().and_then(|s| s.parse::<u32>().ok());
        let dst = it.next().and_then(|s| s.parse::<u32>().ok());
        let weight: f32 = it.next().and_then(|s| s.parse().ok()).unwrap_or(1.0);
        if let (Some(s), Some(d)) = (src, dst) {
            assert!(
                weight.is_finite() && weight >= 0.0,
                "[Worker {worker_id}] {part_key}: negative or NaN edge weight {weight} on {s} -> {d}"
            );
            if s >= num_nodes || d >= num_nodes {
                eprintln!(
                    "[Worker {worker_id}] Invalid edge in {part_key}: {s} -> {d} (max={})",
                    num_nodes.saturating_sub(1)
                );
                continue;
            }
            edges.push((s, d, weight));
        }
    }
}

/// Build a [`CSRGraph`] from an unsorted weighted edge list.
///
/// Edges are sorted by source so the neighbours and weights of node `i`
/// end up contiguous between `offsets[i]` and `offsets[i + 1]` in the
/// parallel `flat_neighbors`/`flat_weights` arrays. Nodes without any
/// out-edge are absent from `owned_nodes`.
fn build_csr_graph(num_nodes: u32, mut edges: Vec<(u32, u32, f32)>) -> CSRGraph {
    edges.sort_unstable_by_key(|e| e.0);
    let mut owned_nodes = Vec::new();
    let mut offsets = vec![0u32; (num_nodes + 1) as usize];
    let mut flat_neighbors = Vec::with_capacity(edges.len());
    let mut flat_weights = Vec::with_capacity(edges.len());
    let mut current_offset = 0u32;
    let mut edge_idx = 0;
    for n in 0..num_nodes {
        offsets[n as usize] = current_offset;
        let mut found = false;
        while edge_idx < edges.len() && edges[edge_idx].0 == n {
            flat_neighbors.push(edges[edge_idx].1);
            flat_weights.push(edges[edge_idx].2);
            edge_idx += 1;
            current_offset += 1;
            found = true;
        }
        if found {
            owned_nodes.push(n);
        }
    }
    offsets[num_nodes as usize] = current_offset;
    CSRGraph {
        owned_nodes,
        offsets,
        flat_neighbors,
        flat_weights,
    }
}

/// Fetch this worker's partition shard from S3 and turn it into a
/// [`CSRGraph`]. Network failures, body-decoding errors, and malformed
/// UTF-8 are surfaced as `Err(String)` so the caller can panic with a
/// clear message after the async task is joined.
async fn load_partition_flat(
    params: &Input,
    s3_client: &S3Client,
    worker_id: u32,
) -> Result<CSRGraph, String> {
    let part_key = format!("{}/part-{:05}", params.input_data.key, worker_id);
    let output = s3_client
        .get_object()
        .bucket(&params.input_data.bucket)
        .key(&part_key)
        .send()
        .await
        .map_err(|err| format!("[Worker {worker_id}] failed to fetch {part_key}: {err}"))?;
    let bytes = output
        .body
        .collect()
        .await
        .map_err(|err| format!("[Worker {worker_id}] failed to read {part_key}: {err}"))?
        .into_bytes();
    let body = std::str::from_utf8(&bytes)
        .map_err(|err| format!("[Worker {worker_id}] invalid UTF-8 in {part_key}: {err}"))?;

    let mut edges = Vec::with_capacity(100_000 * params.granularity as usize);
    parse_partition_body(worker_id, &part_key, body, params.num_nodes, &mut edges);

    let graph = build_csr_graph(params.num_nodes, edges);
    println!(
        "[Worker {worker_id}] Loaded partition {worker_id}: {} owned nodes, {} edges",
        graph.owned_nodes.len(),
        graph.flat_neighbors.len()
    );
    if graph.flat_neighbors.is_empty() {
        // Empty partitions are valid: SSSP still produces a result and the
        // worker contributes nothing to the global distance reduction.
        println!("[Worker {worker_id}] Warning: empty partition; continuing");
    }
    Ok(graph)
}

/// Combine two per-worker distance vectors into one. Last slot carries
/// the relaxation count and is summed; the first `n` slots hold f32 bit
/// patterns and are reduced with `u32::min` (which equals `f32::min` for
/// the non-negative + `+∞` values produced by the loop).
fn merge_distances(mut left: DistanceMessage, right: DistanceMessage) -> DistanceMessage {
    let n = left.0.len() - 1;
    for i in 0..n {
        if right.0[i] < left.0[i] {
            left.0[i] = right.0[i];
        }
    }
    left.0[n] = left.0[n].saturating_add(right.0[n]);
    left
}

/// Render the final SSSP state into a human-readable summary used both
/// for stdout logging and for the `results` field returned by
/// [`ROOT_WORKER`]. The histogram has 10 buckets uniformly partitioning
/// `[0, max_distance]`; an extra `UNREACHABLE` row reports the count of
/// `+∞` entries.
fn build_results_report(
    distances: &[f32],
    num_nodes: u32,
    source: u32,
    reachable: usize,
    max_dist: f32,
    executed_iters: u32,
    total_relaxed: u64,
) -> String {
    let mut report = String::new();
    report.push_str("\n=== SSSP Results ===\n");
    let _ = writeln!(report, "Source node:       {source}");
    let _ = writeln!(report, "Total nodes:       {num_nodes}");
    let _ = writeln!(report, "Reachable nodes:   {reachable}");
    let _ = writeln!(report, "Max distance:      {max_dist:.4}");
    let _ = writeln!(report, "Iterations:        {executed_iters}");
    let _ = writeln!(report, "Total relaxations: {total_relaxed}");

    let bucket_size = if max_dist > 0.0 { max_dist / 10.0 } else { 1.0 };
    let mut buckets = [0u64; 10];
    let mut inf_count = 0u64;
    for &d in distances {
        if d.is_finite() {
            let b = ((d / bucket_size) as usize).min(9);
            buckets[b] += 1;
        } else {
            inf_count += 1;
        }
    }
    report.push_str("\nDistance distribution:\n");
    for (i, &cnt) in buckets.iter().enumerate() {
        if cnt > 0 {
            let lo = i as f32 * bucket_size;
            let hi = (i + 1) as f32 * bucket_size;
            let _ = writeln!(report, "  [{lo:.1}, {hi:.1}): {cnt} nodes");
        }
    }
    if inf_count > 0 {
        let _ = writeln!(report, "  UNREACHABLE: {inf_count} nodes");
    }
    report.push_str("====================\n");
    report
}

/// Core distributed SSSP loop, decoupled from S3 I/O.
///
/// On entry every worker has its local CSR graph and the same `params`.
/// The function:
///
/// 1. Seeds `dist[source] = 0` (when source is in range) and all other
///    slots with `+∞`.
/// 2. Loops: each owned source with finite distance relaxes its
///    out-edges into a local distance buffer (Gauss-Seidel within
///    worker), the local buffers are reduced via [`merge_distances`]
///    and broadcast back as the next global distance vector.
/// 3. Stops when no worker improved any distance or when
///    `max_iterations` is reached.
///
/// Returns `(timestamps, dist_bits, executed_iters, total_relaxed)`
/// where `executed_iters` counts completed relaxation rounds, including
/// the final convergence-check round.
///
/// # Panics
/// - If `params.partitions != burst_size` (broken partition model).
/// - If `worker_id >= burst_size`.
fn run_sssp_core(
    params: &Input,
    middleware: &MiddlewareActorHandle<DistanceMessage>,
    graph: CSRGraph,
    mut timestamps: Vec<Timestamp>,
) -> (Vec<Timestamp>, Vec<u32>, u32, u64) {
    let worker = middleware.info.worker_id;
    let burst_size = middleware.info.burst_size;
    assert_eq!(
        params.partitions, burst_size,
        "partitions ({}) must equal burst_size ({burst_size}); each worker owns exactly one partition",
        params.partitions
    );
    assert!(
        worker < burst_size,
        "worker_id {worker} outside burst_size {burst_size}"
    );

    let source = params.source_node.unwrap_or(0);
    let max_iter = params.max_iterations.unwrap_or(MAX_ITERATIONS);
    let n = params.num_nodes as usize;

    println!(
        "[Worker {worker}] starting SSSP (burst_size={burst_size}, num_nodes={}, source={source})",
        params.num_nodes
    );

    let mut dist_bits = vec![INF_BITS; n];
    if source < params.num_nodes {
        dist_bits[source as usize] = 0.0f32.to_bits();
    } else {
        eprintln!(
            "[Worker {worker}] WARNING: source_node={source} outside num_nodes={}; all distances remain unreachable",
            params.num_nodes
        );
        timestamps.push(timestamp("worker_end"));
        return (timestamps, dist_bits, 0, 0);
    }

    let mut executed_iters: u32 = 0;
    let mut total_relaxed: u64 = 0;
    let extended_size = n + 1;
    let mut converged = false;

    while executed_iters < max_iter {
        timestamps.push(timestamp(&format!("iter_{executed_iters}_start")));

        // Seed the local buffer with the global view, then relax owned
        // out-edges. The snapshot in dist_bits is the source of truth for
        // d_u throughout this iteration.
        let mut local_dist = vec![INF_BITS; extended_size];
        local_dist[..n].copy_from_slice(&dist_bits);
        let mut local_changed: u32 = 0;

        for &node in &graph.owned_nodes {
            let node_idx = node as usize;
            let d_u = f32::from_bits(dist_bits[node_idx]);
            if !d_u.is_finite() {
                continue;
            }
            let start = graph.offsets[node_idx] as usize;
            let end = graph.offsets[node_idx + 1] as usize;
            for i in start..end {
                let v = graph.flat_neighbors[i] as usize;
                let candidate_bits = (d_u + graph.flat_weights[i]).to_bits();
                if candidate_bits < local_dist[v] {
                    local_dist[v] = candidate_bits;
                    local_changed += 1;
                }
            }
        }
        local_dist[n] = local_changed;

        timestamps.push(timestamp(&format!("iter_{executed_iters}_compute")));

        let reduced = middleware
            .reduce(DistanceMessage(local_dist), merge_distances)
            .unwrap();
        timestamps.push(timestamp(&format!("iter_{executed_iters}_reduce")));

        let global = middleware.broadcast(reduced, ROOT_WORKER).unwrap();
        timestamps.push(timestamp(&format!("iter_{executed_iters}_broadcast")));

        let total_changed = global.0[n];
        total_relaxed += u64::from(total_changed);
        dist_bits.copy_from_slice(&global.0[..n]);
        executed_iters += 1;

        if worker == ROOT_WORKER {
            let reachable = dist_bits.iter().filter(|&&b| b != INF_BITS).count();
            println!(
                "[Worker {worker}] iter {}: changed={total_changed}, reachable={reachable}",
                executed_iters - 1
            );
        }

        if total_changed == 0 {
            converged = true;
            break;
        }
    }

    if !converged && executed_iters >= max_iter {
        eprintln!(
            "[Worker {worker}] WARNING: SSSP reached max_iterations={max_iter} without converging"
        );
    }

    timestamps.push(timestamp("worker_end"));
    (timestamps, dist_bits, executed_iters, total_relaxed)
}

/// Per-worker entry point: build the runtime, fetch the partition, run
/// the SSSP core, and (on [`ROOT_WORKER`]) write the distance vector to
/// S3 plus the human-readable summary.
fn sssp(params: Input, middleware: &MiddlewareActorHandle<DistanceMessage>) -> Output {
    let mut timestamps = vec![timestamp("worker_start")];
    let worker = middleware.info.worker_id;

    let credentials_provider = Credentials::from_keys(
        params.input_data.aws_access_key_id.clone(),
        params.input_data.aws_secret_access_key.clone(),
        params.input_data.aws_session_token.clone(),
    );
    let mut builder = aws_sdk_s3::config::Builder::new()
        .credentials_provider(credentials_provider)
        .region(Region::new(params.input_data.region.clone()));
    if let Some(endpoint) = params.input_data.endpoint.clone() {
        builder = builder.endpoint_url(endpoint).force_path_style(true);
    }
    let config = builder.build();
    let s3_client = S3Client::from_conf(config);
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    timestamps.push(timestamp("get_input"));
    let graph = rt
        .block_on(load_partition_flat(&params, &s3_client, worker))
        .unwrap_or_else(|err| panic!("{err}"));
    timestamps.push(timestamp("get_input_end"));

    let (mut timestamps, dist_bits, executed_iters, total_relaxed) =
        run_sssp_core(&params, middleware, graph, timestamps);

    let source = params.source_node.unwrap_or(0);
    let results_report = if worker == ROOT_WORKER {
        timestamps.push(timestamp("write_output_start"));

        let distances: Vec<f32> = dist_bits.iter().map(|&b| f32::from_bits(b)).collect();
        let reachable = distances.iter().filter(|d| d.is_finite()).count();
        let max_dist = distances
            .iter()
            .filter(|d| d.is_finite())
            .copied()
            .fold(0.0f32, f32::max);

        let report = build_results_report(
            &distances,
            params.num_nodes,
            source,
            reachable,
            max_dist,
            executed_iters,
            total_relaxed,
        );
        println!("{report}");

        if params.num_nodes < 10_000_000 {
            let output_key = format!("{}/output/sssp_distances_final.json", params.input_data.key);
            let dist_json = serde_json::json!({
                "source": source,
                "reachable_nodes": reachable,
                "max_distance": max_dist,
                "distances": distances,
            });
            let dist_str = serde_json::to_string(&dist_json).unwrap();
            let write_result = rt.block_on(async {
                s3_client
                    .put_object()
                    .bucket(&params.input_data.bucket)
                    .key(&output_key)
                    .body(dist_str.into_bytes().into())
                    .send()
                    .await
            });
            match write_result {
                Ok(_) => println!(
                    "[Worker {worker}] ✓ Wrote distances to s3://{}/{output_key}",
                    params.input_data.bucket
                ),
                Err(e) => eprintln!("[Worker {worker}] ✗ Failed to write distances: {e:?}"),
            }
        } else {
            println!(
                "[Worker {worker}] ! Skipping S3 write for large graph ({} nodes)",
                params.num_nodes
            );
        }

        timestamps.push(timestamp("write_output_end"));
        Some(report)
    } else {
        None
    };

    Output {
        bucket: params.input_data.bucket.clone(),
        key: format!("worker-{worker}"),
        timestamps,
        results: results_report,
    }
}

/// Action entry point invoked by the Burst harness.
///
/// Deserialises the JSON `Input`, validates the
/// `partitions / granularity` constraint, then delegates to `sssp` and
/// re-serialises its `Output`.
///
/// # Errors
/// Returns `Err` if the input JSON cannot be deserialised into `Input` or
/// if the `Output` cannot be re-serialised back to JSON.
///
/// # Panics
/// If `partitions % granularity != 0`, or if any inner panic surfaces
/// from the SSSP core (e.g. S3 fetch failure, partition/burst mismatch).
pub fn main(args: Value, burst_middleware: Middleware<DistanceMessage>) -> Result<Value, Error> {
    let input: Input = serde_json::from_value(args)?;
    assert!(
        input.partitions % input.granularity == 0,
        "partitions ({}) must be divisible by granularity ({})",
        input.partitions,
        input.granularity
    );
    let handle = burst_middleware.get_actor_handle();
    let result = sssp(input, &handle);
    serde_json::to_value(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use burst_communication_middleware::{
        BurstMiddleware, BurstOptions, Middleware, RemoteBroadcastProxy, RemoteMessage,
        RemoteSendReceiveFactory, RemoteSendReceiveProxy, TokioChannelImpl, TokioChannelOptions,
    };
    use std::{
        collections::{HashMap as StdHashMap, HashSet},
        sync::Arc,
        thread,
    };

    /// Stand-in for the cross-host RPC proxies. In-process two-worker tests
    /// share a single `BurstMiddleware`, so remote send/recv must never be
    /// invoked; the panicking arms below catch that mistake loudly.
    struct DummyRemoteProxy;

    #[async_trait]
    impl burst_communication_middleware::RemoteSendProxy for DummyRemoteProxy {
        async fn remote_send(
            &self,
            _dest: u32,
            _msg: RemoteMessage,
        ) -> burst_communication_middleware::Result<()> {
            Ok(())
        }
    }

    #[async_trait]
    impl burst_communication_middleware::RemoteReceiveProxy for DummyRemoteProxy {
        async fn remote_recv(
            &self,
            _source: u32,
        ) -> burst_communication_middleware::Result<RemoteMessage> {
            panic!("remote recv should not be used in the local distributed SSSP test");
        }
    }

    impl RemoteSendReceiveProxy for DummyRemoteProxy {}

    #[async_trait]
    impl burst_communication_middleware::RemoteBroadcastSendProxy for DummyRemoteProxy {
        async fn remote_broadcast_send(
            &self,
            _msg: RemoteMessage,
        ) -> burst_communication_middleware::Result<()> {
            Ok(())
        }
    }

    #[async_trait]
    impl burst_communication_middleware::RemoteBroadcastReceiveProxy for DummyRemoteProxy {
        async fn remote_broadcast_recv(
            &self,
        ) -> burst_communication_middleware::Result<RemoteMessage> {
            panic!("remote broadcast recv should not be used in the local distributed SSSP test");
        }
    }

    impl RemoteBroadcastProxy for DummyRemoteProxy {}

    /// Factory that hands every worker in the local group the same
    /// [`DummyRemoteProxy`], satisfying `RemoteSendReceiveFactory` without
    /// involving real networking.
    struct DummyRemoteFactory;

    #[async_trait]
    impl RemoteSendReceiveFactory<()> for DummyRemoteFactory {
        async fn create_remote_proxies(
            burst_options: Arc<BurstOptions>,
            _options: (),
        ) -> burst_communication_middleware::Result<
            StdHashMap<
                u32,
                (
                    Box<dyn RemoteSendReceiveProxy>,
                    Box<dyn RemoteBroadcastProxy>,
                ),
            >,
        > {
            let current_group = burst_options
                .group_ranges
                .get(&burst_options.group_id)
                .unwrap();
            Ok(current_group
                .iter()
                .map(|worker_id| {
                    (
                        *worker_id,
                        (
                            Box::new(DummyRemoteProxy) as Box<dyn RemoteSendReceiveProxy>,
                            Box::new(DummyRemoteProxy) as Box<dyn RemoteBroadcastProxy>,
                        ),
                    )
                })
                .collect())
        }
    }

    fn bits_to_f32(b: u32) -> f32 {
        f32::from_bits(b)
    }

    #[test]
    fn distance_message_roundtrips_through_bytes() {
        let data: Vec<u32> = vec![
            0.0f32.to_bits(),
            1.5f32.to_bits(),
            INF_BITS,
            3.14f32.to_bits(),
            42,
        ];
        let msg = DistanceMessage(data.clone());
        let bytes: Bytes = msg.into();
        let back: DistanceMessage = bytes.into();
        assert_eq!(back.0, data);
    }

    #[test]
    fn distance_message_empty_roundtrip() {
        let msg = DistanceMessage(vec![]);
        let bytes: Bytes = msg.into();
        assert!(bytes.is_empty());
        let back: DistanceMessage = bytes.into();
        assert!(back.0.is_empty());
    }

    #[test]
    #[should_panic(expected = "not divisible by 4")]
    fn distance_message_rejects_misaligned_input() {
        let _ = DistanceMessage::from(Bytes::from(vec![0u8, 1, 2]));
    }

    #[test]
    fn bit_ordering_matches_f32_for_nonneg() {
        // Critical invariant: `u32::min` on bits == `f32::min` for non-negative
        // values plus +∞. Verify with a representative ladder.
        let vals = [0.0f32, 0.001, 0.1, 1.0, 10.0, 100.0, 1e10, f32::INFINITY];
        for w in vals.windows(2) {
            assert!(
                w[0].to_bits() < w[1].to_bits(),
                "violation at {} vs {}",
                w[0],
                w[1]
            );
        }
    }

    #[test]
    fn merge_distances_takes_min_per_slot_and_sums_count() {
        let left = DistanceMessage(vec![5.0f32.to_bits(), INF_BITS, 2.0f32.to_bits(), 7]);
        let right = DistanceMessage(vec![3.0f32.to_bits(), 4.0f32.to_bits(), INF_BITS, 5]);
        let out = merge_distances(left, right);
        assert_eq!(bits_to_f32(out.0[0]), 3.0);
        assert_eq!(bits_to_f32(out.0[1]), 4.0);
        assert_eq!(bits_to_f32(out.0[2]), 2.0);
        assert_eq!(out.0[3], 12);
    }

    #[test]
    fn merge_distances_count_saturates() {
        let left = DistanceMessage(vec![INF_BITS, u32::MAX - 5]);
        let right = DistanceMessage(vec![INF_BITS, 100]);
        let out = merge_distances(left, right);
        assert_eq!(out.0[1], u32::MAX);
    }

    #[test]
    fn parse_partition_body_skips_invalid_and_defaults_weight() {
        // (1,99) out of range → skipped. Last line has no weight column → defaults to 1.0.
        let body = "0\t1\t2.5\n1\t99\t1.0\nbad line\n2\t0\n";
        let mut edges = Vec::new();
        parse_partition_body(0, "p", body, 4, &mut edges);
        assert_eq!(edges.len(), 2);
        assert_eq!(edges[0], (0, 1, 2.5));
        assert_eq!(edges[1].0, 2);
        assert_eq!(edges[1].1, 0);
        assert!((edges[1].2 - 1.0).abs() < 1e-6);
    }

    #[test]
    #[should_panic(expected = "negative or NaN edge weight")]
    fn parse_partition_body_panics_on_negative_weight() {
        let mut edges = Vec::new();
        parse_partition_body(0, "p", "0\t1\t-3.5\n", 2, &mut edges);
    }

    #[test]
    fn build_csr_graph_handles_unsorted_and_isolated() {
        let g = build_csr_graph(4, vec![(2, 0, 1.0), (0, 1, 2.0), (2, 1, 3.0), (0, 2, 4.0)]);
        assert_eq!(g.owned_nodes, vec![0, 2]);
        assert_eq!(g.offsets, vec![0, 2, 2, 4, 4]);
        // Node 0's neighbours are (1,2.0) and (2,4.0) in some order.
        let mut neigh0: Vec<(u32, f32)> = g.flat_neighbors[0..2]
            .iter()
            .zip(g.flat_weights[0..2].iter())
            .map(|(&n, &w)| (n, w))
            .collect();
        neigh0.sort_by_key(|&(n, _)| n);
        assert_eq!(neigh0, vec![(1, 2.0), (2, 4.0)]);
    }

    #[test]
    fn build_csr_graph_empty_edges() {
        let g = build_csr_graph(3, vec![]);
        assert!(g.owned_nodes.is_empty());
        assert!(g.flat_neighbors.is_empty());
        assert!(g.flat_weights.is_empty());
        assert_eq!(g.offsets, vec![0, 0, 0, 0]);
    }

    #[test]
    fn build_results_report_renders_key_stats() {
        let distances = vec![0.0, 1.0, 3.0, f32::INFINITY];
        let report = build_results_report(&distances, 4, 0, 3, 3.0, 2, 5);
        assert!(report.contains("Source node:       0"));
        assert!(report.contains("Total nodes:       4"));
        assert!(report.contains("Reachable nodes:   3"));
        assert!(report.contains("Iterations:        2"));
        assert!(report.contains("Total relaxations: 5"));
        assert!(report.contains("UNREACHABLE: 1 nodes"));
    }

    fn make_test_params(num_nodes: u32, source: u32, max_iter: u32) -> Input {
        Input {
            input_data: S3InputParams {
                bucket: "unused".to_string(),
                key: "unused".to_string(),
                region: "us-east-1".to_string(),
                endpoint: None,
                aws_access_key_id: "unused".to_string(),
                aws_secret_access_key: "unused".to_string(),
                aws_session_token: None,
            },
            num_nodes,
            source_node: Some(source),
            max_iterations: Some(max_iter),
            partitions: 2,
            granularity: 1,
            group_id: None,
            timeout_seconds: None,
        }
    }

    /// Spin up a 2-worker `BurstMiddleware` in a multi-threaded Tokio
    /// runtime, hand each worker its CSR graph, run [`run_sssp_core`] to
    /// completion, and return both workers' final distance vectors plus
    /// the iteration / relaxation counters.
    fn run_two_worker_sssp(
        burst_id: &str,
        params: Input,
        worker0_graph: CSRGraph,
        worker1_graph: CSRGraph,
    ) -> ((Vec<f32>, u32, u64), (Vec<f32>, u32, u64)) {
        let group_ranges = vec![(0.to_string(), vec![0, 1].into_iter().collect())]
            .into_iter()
            .collect::<StdHashMap<String, HashSet<u32>>>();

        let tokio_runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();

        let proxies = tokio_runtime
            .block_on(BurstMiddleware::create_proxies::<
                TokioChannelImpl,
                DummyRemoteFactory,
                _,
                _,
            >(
                BurstOptions::new(2, group_ranges, 0.to_string())
                    .burst_id(burst_id.to_string())
                    .enable_message_chunking(false)
                    .build(),
                TokioChannelOptions::new()
                    .broadcast_channel_size(32)
                    .build(),
                (),
            ))
            .unwrap();

        let mut actors = proxies
            .into_iter()
            .map(|(worker_id, middleware)| {
                (
                    worker_id,
                    Middleware::new(middleware, tokio_runtime.handle().clone()),
                )
            })
            .collect::<StdHashMap<u32, Middleware<DistanceMessage>>>();

        let handle0 = actors.remove(&0).unwrap().get_actor_handle();
        let handle1 = actors.remove(&1).unwrap().get_actor_handle();
        let params0 = params.clone();
        let params1 = params;

        let thread0 = thread::spawn(move || {
            run_sssp_core(&params0, &handle0, worker0_graph, vec![timestamp("ws")])
        });
        let thread1 = thread::spawn(move || {
            run_sssp_core(&params1, &handle1, worker1_graph, vec![timestamp("ws")])
        });
        let (_, b0, e0, r0) = thread0.join().unwrap();
        let (_, b1, e1, r1) = thread1.join().unwrap();
        let d0 = b0.iter().map(|&b| f32::from_bits(b)).collect();
        let d1 = b1.iter().map(|&b| f32::from_bits(b)).collect();
        ((d0, e0, r0), (d1, e1, r1))
    }

    #[test]
    fn distributed_sssp_chain_propagates_distances() {
        // Chain 0 → 1 (w=1) → 2 (w=2) → 3 (w=3). Worker0 owns 0,2; worker1 owns 1.
        let params = make_test_params(4, 0, 100);
        let g0 = build_csr_graph(4, vec![(0, 1, 1.0), (2, 3, 3.0)]);
        let g1 = build_csr_graph(4, vec![(1, 2, 2.0)]);
        let ((d0, _, _), (d1, _, _)) = run_two_worker_sssp("sssp-chain", params, g0, g1);
        assert_eq!(d0, vec![0.0, 1.0, 3.0, 6.0]);
        assert_eq!(d1, vec![0.0, 1.0, 3.0, 6.0]);
    }

    #[test]
    fn distributed_sssp_picks_shortest_path() {
        // Diamond: 0→1 (w=1), 0→2 (w=5), 1→3 (w=2), 2→3 (w=1).
        // Shortest 0→3 is via 1: 1 + 2 = 3. Worker split: w0 owns 0,1; w1 owns 2.
        let params = make_test_params(4, 0, 100);
        let g0 = build_csr_graph(4, vec![(0, 1, 1.0), (0, 2, 5.0), (1, 3, 2.0)]);
        let g1 = build_csr_graph(4, vec![(2, 3, 1.0)]);
        let ((d0, _, _), _) = run_two_worker_sssp("sssp-diamond", params, g0, g1);
        assert_eq!(d0[0], 0.0);
        assert_eq!(d0[1], 1.0);
        assert_eq!(d0[2], 5.0);
        assert_eq!(d0[3], 3.0);
    }

    #[test]
    fn distributed_sssp_unreachable_node_stays_inf() {
        let params = make_test_params(3, 0, 100);
        let g0 = build_csr_graph(3, vec![(0, 1, 1.0)]);
        let g1 = build_csr_graph(3, vec![]);
        let ((d, _, _), _) = run_two_worker_sssp("sssp-disc", params, g0, g1);
        assert_eq!(d[0], 0.0);
        assert_eq!(d[1], 1.0);
        assert!(d[2].is_infinite());
    }

    #[test]
    fn distributed_sssp_source_out_of_range_yields_all_inf() {
        let params = make_test_params(3, 99, 100);
        let g0 = build_csr_graph(3, vec![(0, 1, 1.0)]);
        let g1 = build_csr_graph(3, vec![]);
        let ((d, iters, relaxed), _) = run_two_worker_sssp("sssp-oor", params, g0, g1);
        assert!(d.iter().all(|x| x.is_infinite()));
        assert_eq!(iters, 0);
        assert_eq!(relaxed, 0);
    }

    #[test]
    fn distributed_sssp_self_loop_does_not_relax_source() {
        // Self-loop on the source plus a useful edge.
        let params = make_test_params(2, 0, 100);
        let g0 = build_csr_graph(2, vec![(0, 0, 5.0), (0, 1, 1.0)]);
        let g1 = build_csr_graph(2, vec![]);
        let ((d, _, _), _) = run_two_worker_sssp("sssp-self", params, g0, g1);
        assert_eq!(d[0], 0.0);
        assert_eq!(d[1], 1.0);
    }

    #[test]
    fn distributed_sssp_cycle_terminates() {
        // 0 → 1 (w=1), 1 → 2 (w=1), 2 → 0 (w=1). Non-negative cycle:
        // distances stabilise at 0,1,2 without looping forever.
        let params = make_test_params(3, 0, 100);
        let g0 = build_csr_graph(3, vec![(0, 1, 1.0), (2, 0, 1.0)]);
        let g1 = build_csr_graph(3, vec![(1, 2, 1.0)]);
        let ((d, _, _), _) = run_two_worker_sssp("sssp-cycle", params, g0, g1);
        assert_eq!(d, vec![0.0, 1.0, 2.0]);
    }

    #[test]
    fn distributed_sssp_max_iter_cap_truncates_traversal() {
        // Long chain of 5 nodes with cap = 2. Only nodes within 2
        // relaxation rounds of the source must be reached.
        let params = make_test_params(5, 0, 2);
        let g0 = build_csr_graph(5, vec![(0, 1, 1.0), (2, 3, 1.0)]);
        let g1 = build_csr_graph(5, vec![(1, 2, 1.0), (3, 4, 1.0)]);
        let ((d, iters, _), _) = run_two_worker_sssp("sssp-cap", params, g0, g1);
        assert_eq!(d[0], 0.0);
        assert!(d[1].is_finite());
        assert!(d[2].is_finite());
        // Nodes 3 and 4 are beyond reach within 2 iterations.
        assert!(d[3].is_infinite());
        assert!(d[4].is_infinite());
        assert_eq!(iters, 2);
    }

    #[test]
    fn run_sssp_core_panics_on_partition_burst_mismatch() {
        let mut params = make_test_params(2, 0, 100);
        params.partitions = 99;
        let g0 = build_csr_graph(2, vec![(0, 1, 1.0)]);
        let g1 = build_csr_graph(2, vec![]);
        let result =
            std::panic::catch_unwind(|| run_two_worker_sssp("sssp-mismatch", params, g0, g1));
        assert!(result.is_err(), "expected partition mismatch to panic");
    }
}
