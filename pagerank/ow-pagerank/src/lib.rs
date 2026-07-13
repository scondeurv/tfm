//! Distributed PageRank action.
//!
//! # Overview
//!
//! Power-iteration PageRank over a directed graph distributed across a fleet
//! of Burst workers. Each worker:
//!
//! 1. Fetches its assigned graph partition from S3.
//! 2. Builds a local Compressed-Sparse-Row (CSR) representation.
//! 3. At every iteration, distributes `rank[u] / out_degree[u]` from each
//!    locally-owned source vertex `u` to its out-neighbours, accumulating
//!    into a partial contributions buffer.
//! 4. Synchronises with peers via `reduce` + `broadcast`. The reduce
//!    operator is element-wise **sum** of f32 values (decoded from `u32`
//!    bits on the wire because the middleware payload type is
//!    `Vec<u32>`).
//! 5. Applies the teleport + damping update locally — identical on every
//!    worker because the broadcast vector is identical, so no further
//!    reduction is needed for termination.
//! 6. Stops when `delta = sum |rank_new - rank_old|` falls below
//!    `tolerance`, or when `max_iterations` is reached.
//!
//! Worker `0` (the `ROOT_WORKER`) writes the final per-node rank vector to
//! S3 and emits a human-readable summary.
//!
//! # Partition model
//!
//! - Exactly one global partition per worker (`partitions == burst_size`).
//! - Source-vertex ownership: a vertex `u` is "owned" by a worker iff at
//!   least one of `u`'s out-edges appears in that worker's partition
//!   file. Synthetic datasets (`setup_large_pagerank_data.py`) ensure
//!   every vertex has `density` out-edges, all sharded by `src %
//!   partitions`, so every vertex has exactly one owning worker.
//!
//! # Dangling nodes
//!
//! Dangling-node mass **is** redistributed uniformly, matching the
//! `standalone`, `rayon`, and `mpi` backends. A vertex is globally
//! dangling iff no worker owns an out-edge for it, so ownership alone
//! cannot detect it (no worker owns a dangling vertex). Instead, a
//! one-time presence reduce at startup identifies the global dangling
//! set; thereafter — because every worker holds the identical full
//! `rank` vector after each broadcast — each worker computes the global
//! dangling mass locally, with **zero** extra per-iteration
//! communication. This reproduces the value MPI obtains via a
//! per-iteration `Allreduce(SUM)`.
//!
//! # Wire format
//!
//! Contributions travel as [`ContribMessage`] — a `Vec<u32>` of length
//! `num_nodes + 1`. The first `num_nodes` slots carry the f32 bit
//! patterns of the per-node partial contributions; the trailing slot
//! piggybacks the local change counter for cheap convergence
//! instrumentation. Conversion to/from [`Bytes`] uses explicit
//! little-endian encoding.

use std::{
    fmt::Write as _,
    sync::{Arc, Mutex, OnceLock},
    time::{SystemTime, UNIX_EPOCH},
};

use aws_config::Region;
use aws_credential_types::Credentials;
use aws_sdk_s3::Client as S3Client;
use burst_communication_middleware::{Middleware, MiddlewareActorHandle};
use bytes::Bytes;
use serde_derive::{Deserialize, Serialize};
use serde_json::{Error, Value};

/// Worker that hosts the canonical aggregation role.
const ROOT_WORKER: u32 = 0;

/// Default cap on power-iteration steps.
const MAX_ITERATIONS: u32 = 100;

/// Default damping factor (McSherry convention).
const DEFAULT_DAMPING: f32 = 0.85;

/// Default L1 convergence tolerance.
const DEFAULT_TOLERANCE: f32 = 1e-6;

/// JSON-serialisable input the action receives from the Burst harness.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Input {
    input_data: S3InputParams,
    num_nodes: u32,
    max_iterations: Option<u32>,
    damping: Option<f32>,
    tolerance: Option<f32>,
    partitions: u32,
    granularity: u32,
    #[serde(default)]
    group_id: Option<u32>,
    timeout_seconds: Option<u64>,
    /// Enable the in-memory burst-local partition cache (defaults to `true`).
    /// Set to `false` to force the S3 fetch + parse + CSR build on every
    /// invocation, reproducing the un-cached deployment behaviour.
    #[serde(default)]
    use_cache: Option<bool>,
}

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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Output {
    bucket: String,
    key: String,
    timestamps: Vec<Timestamp>,
    #[serde(skip_serializing_if = "Option::is_none")]
    results: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Timestamp {
    key: String,
    value: String,
}

fn timestamp(key: &str) -> Timestamp {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis();
    Timestamp { key: key.to_string(), value: now.to_string() }
}

/// Partial contributions exchanged each iteration. Layout: a `Vec<u32>`
/// of length `num_nodes + 1`. First `num_nodes` slots are f32 bit
/// patterns of per-node partial contribution; trailing slot is a local
/// change counter (always 1 from a worker's own perspective, summed
/// across the fleet for diagnostics).
#[derive(Debug, Clone, PartialEq)]
pub struct ContribMessage(pub Vec<u32>);

impl From<Bytes> for ContribMessage {
    fn from(bytes: Bytes) -> Self {
        if bytes.is_empty() {
            return ContribMessage(vec![]);
        }
        assert!(
            bytes.len() % 4 == 0,
            "ContribMessage byte length {} not divisible by 4",
            bytes.len()
        );
        ContribMessage(
            bytes
                .chunks_exact(4)
                .map(|chunk| u32::from_le_bytes(chunk.try_into().unwrap()))
                .collect(),
        )
    }
}

impl From<ContribMessage> for Bytes {
    fn from(val: ContribMessage) -> Self {
        let mut bytes = Vec::with_capacity(val.0.len() * 4);
        for value in val.0 {
            bytes.extend_from_slice(&value.to_le_bytes());
        }
        Bytes::from(bytes)
    }
}

struct CSRGraph {
    owned_nodes: Vec<u32>,
    offsets: Vec<u32>,
    flat_neighbors: Vec<u32>,
}

fn parse_partition_body(
    worker_id: u32,
    part_key: &str,
    body: &str,
    num_nodes: u32,
    edges: &mut Vec<(u32, u32)>,
) {
    for line in body.lines() {
        if line.trim().is_empty() || line.starts_with('#') {
            continue;
        }
        let mut it = line.split('\t');
        let src = it.next().and_then(|s| s.parse::<u32>().ok());
        let dst = it.next().and_then(|s| s.parse::<u32>().ok());
        if let (Some(s), Some(d)) = (src, dst) {
            if s >= num_nodes || d >= num_nodes {
                eprintln!(
                    "[Worker {worker_id}] Invalid edge in {part_key}: {s} -> {d} (max={})",
                    num_nodes.saturating_sub(1)
                );
                continue;
            }
            edges.push((s, d));
        }
    }
}

fn build_csr_graph(num_nodes: u32, mut edges: Vec<(u32, u32)>) -> CSRGraph {
    edges.sort_unstable_by_key(|e| e.0);
    let mut owned_nodes = Vec::new();
    let mut offsets = vec![0u32; (num_nodes + 1) as usize];
    let mut flat_neighbors = Vec::with_capacity(edges.len());
    let mut current_offset = 0u32;
    let mut edge_idx = 0;
    for n in 0..num_nodes {
        offsets[n as usize] = current_offset;
        let mut found = false;
        while edge_idx < edges.len() && edges[edge_idx].0 == n {
            flat_neighbors.push(edges[edge_idx].1);
            edge_idx += 1;
            current_offset += 1;
            found = true;
        }
        if found {
            owned_nodes.push(n);
        }
    }
    offsets[num_nodes as usize] = current_offset;
    CSRGraph { owned_nodes, offsets, flat_neighbors }
}

/// Cross-invocation, burst-local partition cache (the in-memory cache
/// proposed by the Burst Computing paper). The ActionLoop runtime keeps
/// this process alive across warm invocations, so a warm burst that
/// re-requests the same partition skips the S3 fetch + parse + CSR build
/// entirely. Keyed by `{key}/part-{worker:05}`; all workers of the pack
/// share the map (they run as threads of this process). Entries whose key
/// does not start with the current dataset prefix are evicted on lookup,
/// so a size sweep cannot accumulate CSRs beyond the container memory
/// limit.
static PARTITION_CACHE: OnceLock<Mutex<std::collections::HashMap<String, Arc<CSRGraph>>>> =
    OnceLock::new();

fn partition_cache_lookup(part_key: &str, dataset_prefix: &str) -> Option<Arc<CSRGraph>> {
    let cache = PARTITION_CACHE.get_or_init(|| Mutex::new(std::collections::HashMap::new()));
    let mut map = cache.lock().unwrap();
    map.retain(|k, _| k.starts_with(dataset_prefix));
    map.get(part_key).cloned()
}

fn partition_cache_store(part_key: String, graph: Arc<CSRGraph>) {
    let cache = PARTITION_CACHE.get_or_init(|| Mutex::new(std::collections::HashMap::new()));
    cache.lock().unwrap().insert(part_key, graph);
}

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
        "[Worker {worker_id}] Loaded partition {worker_id}: {} owned source nodes, {} edges",
        graph.owned_nodes.len(),
        graph.flat_neighbors.len()
    );
    if graph.flat_neighbors.is_empty() {
        println!("[Worker {worker_id}] Warning: empty partition; continuing");
    }
    Ok(graph)
}

/// Combine two contribution vectors slot-wise. The first `n` slots are
/// f32 bit patterns reduced via decode-sum-encode; the trailing slot is
/// a saturating-add change counter.
fn merge_contribs(mut left: ContribMessage, right: ContribMessage) -> ContribMessage {
    let n = left.0.len() - 1;
    for i in 0..n {
        let l = f32::from_bits(left.0[i]);
        let r = f32::from_bits(right.0[i]);
        left.0[i] = (l + r).to_bits();
    }
    left.0[n] = left.0[n].saturating_add(right.0[n]);
    left
}

/// Scatter the rank of every locally-owned source vertex to its
/// out-neighbours, accumulating per-destination partial contributions.
/// Returns a `Vec<f32>` of length `n` (one slot per node). Pure: depends
/// only on the local partition and the current rank, so it is unit
/// testable without the middleware.
fn scatter_local(graph: &CSRGraph, rank: &[f32], n: usize) -> Vec<f32> {
    let mut contrib = vec![0.0f32; n];
    for &u in &graph.owned_nodes {
        let uidx = u as usize;
        let start = graph.offsets[uidx] as usize;
        let end = graph.offsets[uidx + 1] as usize;
        let deg = (end - start) as u32;
        if deg == 0 {
            continue;
        }
        let share = rank[uidx] / deg as f32;
        for k in start..end {
            let v = graph.flat_neighbors[k] as usize;
            if v < n {
                contrib[v] += share;
            }
        }
    }
    contrib
}

/// Apply the teleport + damping + dangling-redistribution update in place,
/// returning the L1 delta `Σ|rank_new - rank_old|`. `global_contrib` is the
/// fleet-summed per-node contribution; `dangling_nodes` is the globally
/// dangling set (computed once). Pure and middleware-free, so the exact
/// production update is exercised directly by tests.
fn apply_pagerank_update(
    rank: &mut [f32],
    global_contrib: &[f32],
    dangling_nodes: &[u32],
    teleport_base: f32,
    damping: f32,
) -> f32 {
    let n = rank.len();
    let dangling_mass: f32 = dangling_nodes.iter().map(|&i| rank[i as usize]).sum();
    let dangling_per_node = dangling_mass / n as f32;
    let mut delta = 0.0f32;
    for i in 0..n {
        let new_v = teleport_base + damping * (global_contrib[i] + dangling_per_node);
        delta += (new_v - rank[i]).abs();
        rank[i] = new_v;
    }
    delta
}

fn build_results_report(
    rank: &[f32],
    num_nodes: u32,
    executed_iters: u32,
    converged: bool,
    damping: f32,
) -> String {
    let mut report = String::new();
    report.push_str("\n=== PageRank Results ===\n");
    let _ = writeln!(report, "Total nodes:       {num_nodes}");
    let _ = writeln!(report, "Iterations:        {executed_iters}");
    let _ = writeln!(report, "Converged:         {converged}");
    let _ = writeln!(report, "Damping:           {damping}");

    let max_rank = rank.iter().cloned().fold(0.0f32, f32::max);
    let sum_rank: f64 = rank.iter().map(|&r| r as f64).sum();
    let _ = writeln!(report, "Max rank:          {max_rank:.6}");
    let _ = writeln!(report, "Sum rank:          {sum_rank:.6}");

    let bucket_size = if max_rank > 0.0 { max_rank / 10.0 } else { 1.0 };
    let mut buckets = [0u64; 10];
    for &r in rank {
        let b = ((r / bucket_size) as usize).min(9);
        buckets[b] += 1;
    }
    report.push_str("\nRank distribution:\n");
    for (i, &cnt) in buckets.iter().enumerate() {
        if cnt > 0 {
            let lo = i as f32 * bucket_size;
            let hi = (i + 1) as f32 * bucket_size;
            let _ = writeln!(report, "  [{lo:.4}, {hi:.4}): {cnt} nodes");
        }
    }
    report.push_str("========================\n");
    report
}

/// Core distributed PageRank loop, decoupled from S3 I/O. Returns
/// `(timestamps, rank, executed_iters, converged)`.
fn run_pagerank_core(
    params: &Input,
    middleware: &MiddlewareActorHandle<ContribMessage>,
    graph: &CSRGraph,
    mut timestamps: Vec<Timestamp>,
) -> (Vec<Timestamp>, Vec<f32>, u32, bool) {
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

    let damping = params.damping.unwrap_or(DEFAULT_DAMPING);
    let tolerance = params.tolerance.unwrap_or(DEFAULT_TOLERANCE);
    let max_iter = params.max_iterations.unwrap_or(MAX_ITERATIONS);
    let n = params.num_nodes as usize;
    let n_f = params.num_nodes as f32;
    let teleport_base = (1.0 - damping) / n_f;

    println!(
        "[Worker {worker}] starting PageRank (burst_size={burst_size}, num_nodes={}, damping={damping})",
        params.num_nodes
    );

    let mut rank = vec![1.0f32 / n_f; n];
    let mut executed_iters: u32 = 0;
    let mut converged = false;

    // Identify globally-dangling vertices (out-degree 0 across the whole
    // fleet). A vertex is owned by a worker iff that worker holds one of
    // its out-edges, so a dangling vertex is owned by nobody and cannot be
    // detected locally. Each worker marks its owned sources as f32 1.0;
    // the fleet-wide sum (reusing `merge_contribs`) is > 0 exactly for
    // vertices that have at least one out-edge somewhere. This collective
    // runs once; the resulting set is static for the whole computation.
    let dangling_nodes: Vec<u32> = {
        let mut presence = vec![0u32; n + 1];
        for &u in &graph.owned_nodes {
            presence[u as usize] = 1.0f32.to_bits();
        }
        presence[n] = 1;
        let reduced = middleware
            .reduce(ContribMessage(presence), merge_contribs)
            .unwrap();
        let global = middleware.broadcast(reduced, ROOT_WORKER).unwrap();
        (0..n as u32)
            .filter(|&i| f32::from_bits(global.0[i as usize]) == 0.0)
            .collect()
    };
    if worker == ROOT_WORKER {
        println!(
            "[Worker {worker}] {} globally-dangling vertices of {n}",
            dangling_nodes.len()
        );
    }

    while executed_iters < max_iter {
        timestamps.push(timestamp(&format!("iter_{executed_iters}_start")));

        // Local scatter → f32 contribution per node, packed into the wire
        // layout (n contribution slots + 1 diagnostic counter slot).
        let local_contrib = scatter_local(&graph, &rank, n);
        let mut local_msg = vec![0u32; n + 1];
        for (slot, &c) in local_msg.iter_mut().take(n).zip(local_contrib.iter()) {
            *slot = c.to_bits();
        }
        local_msg[n] = 1; // diagnostic: number of contributing workers

        timestamps.push(timestamp(&format!("iter_{executed_iters}_compute")));

        let reduced = middleware
            .reduce(ContribMessage(local_msg), merge_contribs)
            .unwrap();
        timestamps.push(timestamp(&format!("iter_{executed_iters}_reduce")));

        let global = middleware.broadcast(reduced, ROOT_WORKER).unwrap();
        timestamps.push(timestamp(&format!("iter_{executed_iters}_broadcast")));

        // Decode the fleet-summed contributions, then apply teleport +
        // damping + dangling redistribution. The dangling mass is computed
        // locally: every worker holds the identical `rank` vector (same
        // input → same update), so the sum is bit-identical fleet-wide and
        // needs no communication.
        let global_contrib: Vec<f32> =
            (0..n).map(|i| f32::from_bits(global.0[i])).collect();
        let delta = apply_pagerank_update(
            &mut rank,
            &global_contrib,
            &dangling_nodes,
            teleport_base,
            damping,
        );
        executed_iters += 1;

        if worker == ROOT_WORKER {
            let max_r = rank.iter().cloned().fold(0.0f32, f32::max);
            println!(
                "[Worker {worker}] iter {}: delta={delta:.6e}, max_rank={max_r:.6}",
                executed_iters - 1
            );
        }

        if delta < tolerance {
            converged = true;
            break;
        }
    }

    if !converged && executed_iters >= max_iter {
        eprintln!(
            "[Worker {worker}] WARNING: PageRank reached max_iterations={max_iter} without converging"
        );
    }

    timestamps.push(timestamp("worker_end"));
    (timestamps, rank, executed_iters, converged)
}

fn pagerank(params: Input, middleware: &MiddlewareActorHandle<ContribMessage>) -> Output {
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
    let use_cache = params.use_cache.unwrap_or(true);
    let part_key = format!("{}/part-{:05}", params.input_data.key, worker);
    let cached = if use_cache {
        partition_cache_lookup(&part_key, &params.input_data.key)
    } else {
        None
    };
    let graph: Arc<CSRGraph> = match cached {
        Some(g) => {
            println!("[Worker {worker}] partition cache HIT for {part_key}");
            timestamps.push(timestamp("get_input_cache_hit"));
            g
        }
        None => {
            let g = Arc::new(
                rt.block_on(load_partition_flat(&params, &s3_client, worker))
                    .unwrap_or_else(|err| panic!("{err}")),
            );
            if use_cache {
                partition_cache_store(part_key, g.clone());
            }
            g
        }
    };
    timestamps.push(timestamp("get_input_end"));

    let damping = params.damping.unwrap_or(DEFAULT_DAMPING);
    let (mut timestamps, rank, executed_iters, converged) =
        run_pagerank_core(&params, middleware, &graph, timestamps);

    let results_report = if worker == ROOT_WORKER {
        timestamps.push(timestamp("write_output_start"));

        let report = build_results_report(
            &rank,
            params.num_nodes,
            executed_iters,
            converged,
            damping,
        );
        println!("{report}");

        if params.num_nodes < 10_000_000 {
            let output_key = format!("{}/output/pagerank_final.json", params.input_data.key);
            let max_rank = rank.iter().cloned().fold(0.0f32, f32::max);
            let sum_rank: f64 = rank.iter().map(|&r| r as f64).sum();
            let dist_json = serde_json::json!({
                "iterations": executed_iters,
                "converged": converged,
                "damping": damping,
                "max_rank": max_rank,
                "sum_rank": sum_rank,
                "rank": rank,
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
                    "[Worker {worker}] ✓ Wrote ranks to s3://{}/{output_key}",
                    params.input_data.bucket
                ),
                Err(e) => eprintln!("[Worker {worker}] ✗ Failed to write ranks: {e:?}"),
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

pub fn main(args: Value, burst_middleware: Middleware<ContribMessage>) -> Result<Value, Error> {
    let input: Input = serde_json::from_value(args)?;
    assert!(
        input.partitions % input.granularity == 0,
        "partitions ({}) must be divisible by granularity ({})",
        input.partitions,
        input.granularity
    );
    let handle = burst_middleware.get_actor_handle();
    let result = pagerank(input, &handle);
    serde_json::to_value(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn contrib_message_roundtrips_through_bytes() {
        let data: Vec<u32> = vec![
            0.0f32.to_bits(),
            0.25f32.to_bits(),
            1.0f32.to_bits(),
            7,
        ];
        let msg = ContribMessage(data.clone());
        let bytes: Bytes = msg.into();
        let back: ContribMessage = bytes.into();
        assert_eq!(back.0, data);
    }

    #[test]
    fn contrib_message_empty_roundtrip() {
        let msg = ContribMessage(vec![]);
        let bytes: Bytes = msg.into();
        assert!(bytes.is_empty());
        let back: ContribMessage = bytes.into();
        assert!(back.0.is_empty());
    }

    #[test]
    #[should_panic(expected = "not divisible by 4")]
    fn contrib_message_rejects_misaligned_input() {
        let _ = ContribMessage::from(Bytes::from(vec![0u8, 1, 2]));
    }

    #[test]
    fn merge_contribs_sums_per_slot() {
        let left = ContribMessage(vec![0.25f32.to_bits(), 0.0f32.to_bits(), 0.5f32.to_bits(), 1]);
        let right = ContribMessage(vec![0.5f32.to_bits(), 0.125f32.to_bits(), 0.0f32.to_bits(), 1]);
        let out = merge_contribs(left, right);
        assert!((f32::from_bits(out.0[0]) - 0.75).abs() < 1e-6);
        assert!((f32::from_bits(out.0[1]) - 0.125).abs() < 1e-6);
        assert!((f32::from_bits(out.0[2]) - 0.5).abs() < 1e-6);
        assert_eq!(out.0[3], 2);
    }

    #[test]
    fn parse_partition_body_skips_invalid_and_comments() {
        let body = "# header\n0\t1\n1\t99\nbad line\n2\t0\n";
        let mut edges = Vec::new();
        parse_partition_body(0, "p", body, 4, &mut edges);
        assert_eq!(edges, vec![(0, 1), (2, 0)]);
    }

    /// End-to-end numeric equivalence: a single-worker burst run (where
    /// `reduce`/`broadcast` are the identity, so the collectives drop out)
    /// must reproduce the canonical `pagerank-core` serial kernel bit-for-bit
    /// on a graph that contains a dangling vertex. This exercises the real
    /// production helpers (`scatter_local`, `apply_pagerank_update`) and
    /// proves dangling mass is redistributed (sum of ranks stays ~1.0).
    #[test]
    fn dangling_redistribution_matches_core() {
        // 0→1→2→0 cycle, plus 0→3 and 1→3; vertex 3 is dangling (no out-edges).
        let edges = vec![(0u32, 1u32), (1, 2), (2, 0), (0, 3), (1, 3)];
        let num_nodes = 4u32;
        let n = num_nodes as usize;
        let damping = DEFAULT_DAMPING;
        let tolerance = DEFAULT_TOLERANCE;
        let max_iter = MAX_ITERATIONS;
        let n_f = num_nodes as f32;
        let teleport_base = (1.0 - damping) / n_f;

        // --- burst path (single worker: collectives are identity) ---
        let graph = build_csr_graph(num_nodes, edges.clone());
        // Single-worker dangling set == complement of owned (out-edge) nodes,
        // identical to what the fleet-wide presence reduce yields for n_w=1.
        let dangling_nodes: Vec<u32> = (0..num_nodes)
            .filter(|u| !graph.owned_nodes.contains(u))
            .collect();
        assert_eq!(dangling_nodes, vec![3], "vertex 3 must be detected dangling");

        let mut rank = vec![1.0f32 / n_f; n];
        for _ in 0..max_iter {
            let contrib = scatter_local(&graph, &rank, n);
            let delta = apply_pagerank_update(
                &mut rank,
                &contrib,
                &dangling_nodes,
                teleport_base,
                damping,
            );
            if delta < tolerance {
                break;
            }
        }

        // --- reference path: canonical serial kernel ---
        let csr = pagerank_core::build_csr(num_nodes, &edges);
        let (ref_rank, _) = pagerank_core::run_pagerank(&csr, max_iter, damping, tolerance);

        // Bit-compatible kernels → ranks agree to f32 round-off.
        for i in 0..n {
            assert!(
                (rank[i] - ref_rank[i]).abs() < 1e-5,
                "node {i}: burst {} vs core {}",
                rank[i],
                ref_rank[i]
            );
        }
        // Dangling mass conserved: total rank stays ~1.0.
        let sum: f32 = rank.iter().sum();
        assert!((sum - 1.0).abs() < 1e-3, "rank sum drifted: {sum}");
    }

    #[test]
    fn partition_cache_hit_and_prefix_eviction() {
        let g = Arc::new(build_csr_graph(2, vec![(0, 1)]));
        partition_cache_store("dsA/part-00000".to_string(), g);
        assert!(partition_cache_lookup("dsA/part-00000", "dsA").is_some());
        // A lookup under a different dataset prefix evicts the stale entry.
        assert!(partition_cache_lookup("dsB/part-00000", "dsB").is_none());
        assert!(partition_cache_lookup("dsA/part-00000", "dsA").is_none());
    }

    #[test]
    fn build_csr_graph_groups_by_source() {
        let edges = vec![(2, 3), (0, 1), (0, 2), (2, 0)];
        let csr = build_csr_graph(4, edges);
        assert_eq!(csr.owned_nodes, vec![0, 2]);
        assert_eq!(csr.offsets[0..5], [0, 2, 2, 4, 4]);
        let neigh_0: Vec<u32> = csr.flat_neighbors[0..2].to_vec();
        let neigh_2: Vec<u32> = csr.flat_neighbors[2..4].to_vec();
        let mut s0 = neigh_0;
        s0.sort();
        assert_eq!(s0, vec![1, 2]);
        let mut s2 = neigh_2;
        s2.sort();
        assert_eq!(s2, vec![0, 3]);
    }
}
