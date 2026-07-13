//! Distributed Label Propagation (LP) action.
//!
//! # Overview
//!
//! Implements semi-supervised and unsupervised graph label propagation across
//! a fleet of Burst workers. Each worker:
//!
//! 1. Fetches its assigned graph partition from S3.
//! 2. Builds a local Compressed-Sparse-Row (CSR) representation.
//! 3. Iteratively recomputes labels for its owned nodes from neighbour
//!    majority votes, synchronising with peers via `reduce` + `broadcast`
//!    collectives provided by the Burst middleware.
//! 4. Stops when fewer labels change than `convergence_threshold`, or when
//!    `max_iterations` is reached.
//!
//! Worker `0` (the `ROOT_WORKER`) additionally writes a JSON dump of the
//! final labels to S3 and produces a human-readable results report.
//!
//! # Partition model
//!
//! - Exactly one global partition per worker (`partitions == burst_size`).
//! - Workers may have overlapping ownership of nodes; the reduce step
//!   resolves conflicts deterministically by taking the smallest label
//!   (see `merge_label_updates`).
//! - Seeds duplicated across workers are deduplicated by smallest label too
//!   (see `apply_seed_pairs`).
//!
//! # Wire format
//!
//! Inter-worker messages travel as [`LabelsMessage`] — a `Vec<u32>` encoded as
//! little-endian bytes for compact middleware transport.

use ahash::AHashMap as HashMap;
use std::{
    cmp::Ordering,
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

/// Worker that owns the canonical seed/result aggregation role: hosts the
/// reduce collective and writes the final S3 dump.
const ROOT_WORKER: u32 = 0;

/// Default cap on LP iterations when the input does not specify one.
const MAX_ITER: u32 = 50;

/// Sentinel marking "no label assigned yet" inside the label vector.
/// Picked as `u32::MAX` so it is distinguishable from any valid label id.
const UNKNOWN: u32 = u32::MAX;

/// JSON-serialisable input the action receives from the Burst harness.
///
/// Field semantics:
/// - `partitions` MUST equal the burst size (one partition per worker).
/// - `granularity` is the number of workers per pack/group. `partitions`
///   must be divisible by `granularity` for balanced placement.
/// - Both `max_iterations` and `convergence_threshold` are optional; defaults
///   are `MAX_ITER` and `0` respectively.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Input {
    /// S3 connection details + key prefix where partition files live.
    input_data: S3InputParams,
    /// Total number of vertices in the global graph.
    num_nodes: u32,
    /// Cap on LP iterations. Defaults to [`MAX_ITER`] when `None`.
    max_iterations: Option<u32>,
    /// Stop when iteration `changed` count is `<= threshold`. Defaults to `0`.
    convergence_threshold: Option<u32>,
    /// Number of partitions the graph was sharded into. Equals `burst_size`.
    partitions: u32,
    /// Workers per Burst pack/group. `partitions % granularity == 0`.
    granularity: u32,
    /// Optional group id (multi-pack scheduling). Accepted for forward
    /// compatibility but not consumed by the action itself.
    #[serde(default)]
    group_id: Option<u32>,
    /// Reserved field for a future per-call collective-operation timeout
    /// override. Currently parsed but not applied.
    timeout_seconds: Option<u64>,
    /// Enable the in-memory burst-local partition cache (defaults to `true`).
    /// Set to `false` to force the S3 fetch + parse + CSR build on every
    /// invocation, reproducing the un-cached deployment behaviour.
    #[serde(default)]
    use_cache: Option<bool>,
}

/// S3 endpoint + credentials + key prefix where partition shards live.
///
/// Each shard is fetched from `{bucket}/{key}/part-{worker_id:05}`.
/// `endpoint` is set for non-AWS S3-compatible stores (MinIO, etc.); when
/// present, path-style addressing is forced.
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

/// Wire-level message exchanged between workers through the Burst middleware.
///
/// Layout depends on context: during the propagation loop the first
/// `num_nodes` slots carry per-node labels (or `UNKNOWN`) and the trailing
/// slot carries the worker's local change count. During seed dissemination
/// the vector is a flat `[node, label, node, label, …]` buffer.
///
/// The conversions to/from [`Bytes`] use explicit little-endian encoding. This
/// avoids allocator-layout assumptions while keeping the wire format stable.
#[derive(Debug, Clone, PartialEq)]
pub struct LabelsMessage(pub Vec<u32>);

impl From<Bytes> for LabelsMessage {
    /// Decode a little-endian byte buffer into `Vec<u32>`.
    ///
    /// # Panics
    /// If `bytes.len()` is not a multiple of `4`.
    fn from(bytes: Bytes) -> Self {
        assert!(
            bytes.len() % 4 == 0,
            "LabelsMessage byte length {} not divisible by 4",
            bytes.len()
        );
        LabelsMessage(
            bytes
                .chunks_exact(4)
                .map(|chunk| u32::from_le_bytes(chunk.try_into().unwrap()))
                .collect(),
        )
    }
}

impl From<LabelsMessage> for Bytes {
    /// Encode `Vec<u32>` into little-endian bytes.
    fn from(val: LabelsMessage) -> Self {
        let mut bytes = Vec::with_capacity(val.0.len() * 4);
        for value in val.0 {
            bytes.extend_from_slice(&value.to_le_bytes());
        }
        Bytes::from(bytes)
    }
}

/// Flatten `(node, label)` seeds into a `[node, label, …]` buffer for the
/// initial reduce. Keeping the encoding compact (only seeded nodes) avoids
/// shipping a full `num_nodes`-sized vector per worker before propagation.
fn encode_seed_pairs(seed_pairs: &[(u32, u32)]) -> LabelsMessage {
    LabelsMessage(seed_pairs.iter().flat_map(|&(n, l)| [n, l]).collect())
}

/// Apply a flattened `[node, label, …]` payload onto an in-memory label slice.
///
/// When the same node appears multiple times (e.g. seeded by several workers
/// with different labels) the smallest label wins, making the outcome
/// independent of reduce/concatenation order.
///
/// # Panics
/// - If `encoded_pairs.len()` is odd (malformed payload).
/// - If any node id is `>= labels.len()`.
fn apply_seed_pairs(labels: &mut [u32], encoded_pairs: &[u32]) {
    assert!(
        encoded_pairs.len() % 2 == 0,
        "seed payload must contain node/label pairs"
    );
    for pair in encoded_pairs.chunks_exact(2) {
        let node = pair[0] as usize;
        let label = pair[1];
        assert!(
            node < labels.len(),
            "seed node {node} out of range (num_nodes={})",
            labels.len()
        );
        if labels[node] == UNKNOWN || label < labels[node] {
            labels[node] = label;
        }
    }
}

/// Deduplicate seed labels produced by repeated labelled edges in one shard.
/// If a node appears with multiple labels, keep the smallest one to match the
/// global seed merge rule.
fn dedup_seed_pairs(seed_pairs: Vec<(u32, u32)>) -> Vec<(u32, u32)> {
    let mut by_node = HashMap::with_capacity(seed_pairs.len());
    for (node, label) in seed_pairs {
        by_node
            .entry(node)
            .and_modify(|existing: &mut u32| *existing = (*existing).min(label))
            .or_insert(label);
    }
    let mut deduped: Vec<_> = by_node.into_iter().collect();
    deduped.sort_unstable_by_key(|&(node, _)| node);
    deduped
}

/// Compressed-Sparse-Row representation of the per-worker subgraph.
///
/// - `owned_nodes` lists the source nodes the worker actually has out-edges
///   for; nodes not in this list contribute UNKNOWN to the reduce.
/// - `offsets[i]` is the start of node `i`'s adjacency list in `flat_edges`,
///   `offsets[i+1]` the end. `offsets` has length `num_nodes + 1`.
/// - `flat_edges` is the concatenation of every owned node's out-neighbours.
struct CSRGraph {
    owned_nodes: Vec<u32>,
    offsets: Vec<u32>,
    flat_edges: Vec<u32>,
}

/// Parse one partition's tab-separated body into edge and seed-label vectors.
///
/// Each non-blank line has the form `src \t dst [\t label]`. Lines that fail
/// to parse, or whose endpoints fall outside `[0, num_nodes)`, are skipped
/// with a warning written to `stderr`. Negative labels are skipped silently
/// (treated as "no label"). Results are appended to the caller-supplied
/// `edges` and `initial_labels` accumulators (append-only so the buffers can
/// be reused across calls without reallocating).
fn parse_partition_body(
    worker_id: u32,
    part_key: &str,
    body: &str,
    num_nodes: u32,
    edges: &mut Vec<(u32, u32)>,
    initial_labels: &mut Vec<(u32, u32)>,
) {
    for line in body.lines() {
        if line.trim().is_empty() {
            continue;
        }
        let mut it = line.split('\t');
        let src = it.next().and_then(|s| s.parse::<u32>().ok());
        let dst = it.next().and_then(|s| s.parse::<u32>().ok());
        let label = it.next().and_then(|s| s.parse::<i64>().ok());
        if let (Some(s), Some(d)) = (src, dst) {
            if s >= num_nodes || d >= num_nodes {
                eprintln!(
                    "[Worker {worker_id}] Invalid edge in {part_key}: {s} -> {d} (max={})",
                    num_nodes.saturating_sub(1)
                );
                continue;
            }
            edges.push((s, d));
            if let Some(l) = label {
                if (0..=u32::MAX as i64).contains(&l) {
                    initial_labels.push((s, l as u32));
                } else if l > u32::MAX as i64 {
                    eprintln!(
                        "[Worker {worker_id}] Invalid label in {part_key}: {l} exceeds u32::MAX"
                    );
                }
            }
        }
    }
}

/// Build a [`CSRGraph`] from an unsorted edge list.
///
/// Edges are sorted by source so neighbours of node `i` end up contiguous
/// in `flat_edges` between `offsets[i]` and `offsets[i + 1]`. Nodes without
/// any out-edge are absent from `owned_nodes` (so the worker does not
/// recompute their label and does not contribute UNKNOWN slots for them).
fn build_csr_graph(num_nodes: u32, mut edges: Vec<(u32, u32)>) -> CSRGraph {
    edges.sort_unstable_by_key(|e| e.0);
    let mut owned_nodes = Vec::new();
    let mut offsets = vec![0u32; (num_nodes + 1) as usize];
    let mut flat_edges = Vec::with_capacity(edges.len());
    let mut current_offset = 0u32;
    let mut edge_idx = 0;
    for n in 0..num_nodes {
        offsets[n as usize] = current_offset;
        let mut found = false;
        while edge_idx < edges.len() && edges[edge_idx].0 == n {
            flat_edges.push(edges[edge_idx].1);
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
        flat_edges,
    }
}

/// Render the final label vector into a human-readable summary.
///
/// Includes the iteration count, the 20 smallest-id distinct labels with
/// their occurrence counts, and the labels assigned to the first 20 nodes.
/// Used both for stdout logging and for the `results` field returned to the
/// Burst harness by [`ROOT_WORKER`].
fn build_results_report(final_labels: &[u32], num_nodes: u32, completed_iterations: u32) -> String {
    let mut label_counts: std::collections::HashMap<u32, usize> = std::collections::HashMap::new();
    for &label in final_labels {
        *label_counts.entry(label).or_insert(0) += 1;
    }

    let mut report = String::new();
    report.push_str("\n=== Label Propagation Results ===\n");
    report.push_str(&format!("Total nodes: {}\n", num_nodes));
    report.push_str(&format!("Total iterations: {}\n", completed_iterations));
    report.push_str("\nLabel Distribution:\n");
    let mut sorted_labels: Vec<_> = label_counts.iter().collect();
    sorted_labels.sort_by_key(|&(label, _)| label);
    for (label, count) in sorted_labels.iter().take(20) {
        if **label == UNKNOWN {
            report.push_str(&format!("  UNKNOWN: {} nodes\n", count));
        } else {
            report.push_str(&format!("  Label {}: {} nodes\n", label, count));
        }
    }

    report.push_str("\nSample nodes (first 20):\n");
    for i in 0..20.min(num_nodes as usize) {
        let label = final_labels[i];
        let label_str = if label == UNKNOWN {
            "UNKNOWN".to_string()
        } else {
            label.to_string()
        };
        report.push_str(&format!("  Node {}: Label {}\n", i, label_str));
    }
    report.push_str("=================================\n");
    report
}

/// Cross-invocation, burst-local partition cache (the in-memory cache
/// proposed by the Burst Computing paper). The ActionLoop runtime keeps
/// this process alive across warm invocations, so a warm burst that
/// re-requests the same partition skips the S3 fetch + parse + CSR build
/// entirely. For LP the cached value bundles the CSR graph with the seed
/// `(node, label)` pairs parsed from the same shard. Keyed by
/// `{key}/part-{worker:05}`; all workers of the pack share the map (they
/// run as threads of this process). Entries whose key does not start with
/// the current dataset prefix are evicted on lookup, so a size sweep
/// cannot accumulate CSRs beyond the container memory limit.
type CachedPartition = Arc<(CSRGraph, Vec<(u32, u32)>)>;
static PARTITION_CACHE: OnceLock<Mutex<std::collections::HashMap<String, CachedPartition>>> =
    OnceLock::new();

fn partition_cache_lookup(part_key: &str, dataset_prefix: &str) -> Option<CachedPartition> {
    let cache = PARTITION_CACHE.get_or_init(|| Mutex::new(std::collections::HashMap::new()));
    let mut map = cache.lock().unwrap();
    map.retain(|k, _| k.starts_with(dataset_prefix));
    map.get(part_key).cloned()
}

fn partition_cache_store(part_key: String, entry: CachedPartition) {
    let cache = PARTITION_CACHE.get_or_init(|| Mutex::new(std::collections::HashMap::new()));
    cache.lock().unwrap().insert(part_key, entry);
}

/// Fetch this worker's partition shard from S3 and turn it into a
/// [`CSRGraph`] plus the list of seed `(node, label)` pairs found in the
/// shard. Network failures, body-decoding errors, and malformed UTF-8 are
/// surfaced as `Err(String)` so the caller can panic with a clear message
/// after the async task is joined.
async fn load_partition_flat(
    params: &Input,
    s3_client: &S3Client,
    worker_id: u32,
) -> Result<(CSRGraph, Vec<(u32, u32)>), String> {
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
    let mut initial_labels = Vec::new();
    parse_partition_body(
        worker_id,
        &part_key,
        body,
        params.num_nodes,
        &mut edges,
        &mut initial_labels,
    );
    let initial_labels = dedup_seed_pairs(initial_labels);

    let graph = build_csr_graph(params.num_nodes, edges);
    println!(
        "[Worker {worker_id}] Loaded partition {worker_id}: {} owned nodes, {} edges",
        graph.owned_nodes.len(),
        graph.flat_edges.len()
    );
    Ok((graph, initial_labels))
}

/// Loop-continuation predicate for the propagation main loop.
///
/// Returns `true` while both:
/// - the iteration counter is still below `max_iter` (or [`MAX_ITER`] if `None`), and
/// - more than `threshold` labels changed in the previous round.
fn should_continue(iter: u32, max_iter: Option<u32>, changed: u32, threshold: u32) -> bool {
    iter < max_iter.unwrap_or(MAX_ITER) && changed > threshold
}

/// Combine two per-worker label vectors into one. Last slot is the change count.
/// For each node both sides may carry UNKNOWN (no contribution) or a label;
/// when both contribute, take the smallest so the merge is deterministic
/// regardless of reduce order (tolerates overlapping ownership).
fn merge_label_updates(mut left: LabelsMessage, right: LabelsMessage) -> LabelsMessage {
    let n = left.0.len() - 1;
    for i in 0..n {
        let l = left.0[i];
        let r = right.0[i];
        left.0[i] = if l == UNKNOWN {
            r
        } else if r == UNKNOWN {
            l
        } else {
            l.min(r)
        };
    }
    left.0[n] = left.0[n].saturating_add(right.0[n]);
    left
}

/// Pick the label that appears most often among a node's neighbours.
///
/// `counts` must hold one entry per distinct neighbour label (UNKNOWN is
/// ignored). Ties are broken by smallest label id, which makes the choice
/// deterministic. If `counts` is empty (no labelled neighbour) the node
/// keeps its `current` label. The map is cleared on return so it can be
/// reused for the next node without reallocation.
fn majority_label(counts: &mut HashMap<u32, usize>, current: u32) -> u32 {
    if counts.is_empty() {
        return current;
    }
    let mut best = current;
    let mut best_count = 0usize;
    for (label, count) in counts.iter() {
        if *label == UNKNOWN {
            continue;
        }
        match count.cmp(&best_count) {
            Ordering::Greater => {
                best = *label;
                best_count = *count;
            }
            Ordering::Equal => {
                if *label < best {
                    best = *label;
                }
            }
            Ordering::Less => {}
        }
    }
    counts.clear();
    best
}

/// Core distributed label-propagation loop, decoupled from S3 I/O.
///
/// Steps:
/// 1. Reduce + broadcast the seed `(node, label)` pairs so every worker
///    starts the loop with the same initial label vector.
/// 2. If no seeds exist anywhere, fall back to **unsupervised** mode: each
///    node initially carries its own id as label.
/// 3. Loop: for every owned node, compute the majority label of its
///    neighbours (seeded nodes are pinned), pack the per-worker updates
///    into a single [`LabelsMessage`] and synchronise via reduce + broadcast.
/// 4. Stop when [`should_continue`] returns `false`.
///
/// Returns the appended `timestamps` trace, the final label vector, and the
/// number of completed iterations.
///
/// # Panics
/// - If `params.partitions != burst_size` (broken partition model).
/// - If `worker_id >= burst_size`.
fn run_label_propagation_core(
    params: &Input,
    middleware: &MiddlewareActorHandle<LabelsMessage>,
    graph: &CSRGraph,
    initial_labels_vec: &[(u32, u32)],
    mut timestamps: Vec<Timestamp>,
) -> (Vec<Timestamp>, Vec<u32>, u32) {
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
    println!(
        "[Worker {worker}] starting label propagation (burst_size={burst_size}, num_nodes={})",
        params.num_nodes
    );

    // Share only the seeded nodes so the initial collective stays small.
    let initial_msg = encode_seed_pairs(initial_labels_vec);
    let combined = middleware
        .reduce(initial_msg, |mut left, right| {
            left.0.extend(right.0);
            left
        })
        .unwrap();

    let global_seed_pairs = middleware.broadcast(combined, ROOT_WORKER).unwrap();

    let mut global_labels = LabelsMessage(vec![UNKNOWN; params.num_nodes as usize]);
    apply_seed_pairs(&mut global_labels.0, &global_seed_pairs.0);
    let is_seed: Vec<bool> = global_labels.0.iter().map(|&l| l != UNKNOWN).collect();
    let unsupervised_mode = global_seed_pairs.0.is_empty();

    // Without seeds, start in unsupervised mode using each node ID as its label.
    if unsupervised_mode {
        println!("[Worker {worker}] No initial labels found globally, using unsupervised mode");
        for (idx, label) in global_labels.0.iter_mut().enumerate() {
            *label = idx as u32;
        }
    }

    let max_iter = params.max_iterations.unwrap_or(MAX_ITER);
    let threshold = params.convergence_threshold.unwrap_or(0);
    let mut iter = 0;
    let mut completed_iterations = 0;

    let avg_degree = if graph.owned_nodes.is_empty() {
        10
    } else {
        (graph.flat_edges.len() / graph.owned_nodes.len()).max(1)
    };
    let mut counts_map = HashMap::with_capacity(avg_degree.min(100));

    let mut labels = global_labels.0;
    let n = params.num_nodes as usize;
    let extended_size = n + 1;
    let mut local_updates = vec![UNKNOWN; extended_size];

    // Run the propagation loop until the labels converge or we hit the iteration limit.
    while iter < max_iter {
        timestamps.push(timestamp(&format!("iter_{}_start", iter)));
        local_updates.fill(UNKNOWN);
        let mut local_changed: u32 = 0;

        // Recompute the labels for the nodes owned by this worker.
        for &node in &graph.owned_nodes {
            let idx = node as usize;
            let current_label = labels[idx];

            if !unsupervised_mode && is_seed[idx] {
                local_updates[idx] = current_label;
                continue;
            }

            let start = graph.offsets[idx];
            let end = graph.offsets[idx + 1];

            for i in start..end {
                let neighbor = graph.flat_edges[i as usize] as usize;
                let label = labels[neighbor];
                if label != UNKNOWN {
                    *counts_map.entry(label).or_insert(0) += 1;
                }
            }

            let new_label = majority_label(&mut counts_map, current_label);
            local_updates[idx] = new_label;
            if new_label != current_label {
                local_changed += 1;
            }
        }
        local_updates[n] = local_changed;
        timestamps.push(timestamp(&format!("iter_{}_compute", iter)));

        let reduced = middleware
            .reduce(
                LabelsMessage(std::mem::take(&mut local_updates)),
                merge_label_updates,
            )
            .unwrap();
        timestamps.push(timestamp(&format!("iter_{}_reduce", iter)));

        // Broadcast the merged labels so the next round reads a synchronized state.
        let global = middleware.broadcast(reduced, ROOT_WORKER).unwrap();
        timestamps.push(timestamp(&format!("iter_{}_broadcast", iter)));

        let total_changed = global.0[n];
        labels.copy_from_slice(&global.0[..n]);
        local_updates = global.0;
        local_updates.resize(extended_size, UNKNOWN);
        completed_iterations += 1;

        if worker == ROOT_WORKER {
            println!("[Worker {worker}] iter {iter}: changed={total_changed}");
        }
        if !should_continue(iter, params.max_iterations, total_changed, threshold) {
            break;
        }
        iter += 1;
    }

    timestamps.push(timestamp("worker_end"));
    (timestamps, labels, completed_iterations)
}

/// Per-worker entry point: build the runtime, fetch the partition, run the
/// propagation core, and (on [`ROOT_WORKER`]) write the final JSON output to
/// S3 plus emit the human-readable summary.
fn label_propagation(params: Input, middleware: &MiddlewareActorHandle<LabelsMessage>) -> Output {
    // Set up the worker runtime and the clients needed for this invocation.
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

    // Load this worker's partition shard from S3 and build the local CSR
    // graph, or reuse the burst-local cached copy on a warm invocation.
    timestamps.push(timestamp("get_input"));
    let use_cache = params.use_cache.unwrap_or(true);
    let part_key = format!("{}/part-{:05}", params.input_data.key, worker);
    let cached = if use_cache {
        partition_cache_lookup(&part_key, &params.input_data.key)
    } else {
        None
    };
    let partition: CachedPartition = match cached {
        Some(entry) => {
            println!("[Worker {worker}] partition cache HIT for {part_key}");
            timestamps.push(timestamp("get_input_cache_hit"));
            entry
        }
        None => {
            let entry = Arc::new(
                rt.block_on(load_partition_flat(&params, &s3_client, worker))
                    .unwrap_or_else(|err| panic!("{err}")),
            );
            if use_cache {
                partition_cache_store(part_key, entry.clone());
            }
            entry
        }
    };
    timestamps.push(timestamp("get_input_end"));

    let (graph, initial_labels_vec) = (&partition.0, &partition.1[..]);
    let (mut timestamps, final_labels, completed_iterations) =
        run_label_propagation_core(&params, middleware, graph, initial_labels_vec, timestamps);

    // Worker 0 writes the final labels and emits the summary used by validation and benchmarks.
    let results_report = if worker == ROOT_WORKER {
        timestamps.push(timestamp("write_labels_start"));

        let report = build_results_report(&final_labels, params.num_nodes, completed_iterations);
        println!("{}", report);

        let output_key = format!("{}/output/labels_final.json", params.input_data.key);

        if params.num_nodes < 10_000_000 {
            let labels_map: std::collections::HashMap<String, u32> = (0..params.num_nodes)
                .map(|i| (i.to_string(), final_labels[i as usize]))
                .collect();
            let labels_json = serde_json::json!({ "labels": labels_map });
            let labels_str = serde_json::to_string(&labels_json).unwrap();
            let write_result = rt.block_on(async {
                s3_client
                    .put_object()
                    .bucket(&params.input_data.bucket)
                    .key(&output_key)
                    .body(labels_str.into_bytes().into())
                    .send()
                    .await
            });
            match write_result {
                Ok(_) => println!(
                    "[Worker {}] ✓ Wrote final labels to s3://{}/{}",
                    worker, params.input_data.bucket, output_key
                ),
                Err(e) => eprintln!("[Worker {}] ✗ Failed to write labels: {:?}", worker, e),
            }
        } else {
            println!(
                "[Worker {}] ! Skipping large JSON serialization for S3 ({} nodes)",
                worker, params.num_nodes
            );
        }

        timestamps.push(timestamp("write_labels_end"));
        Some(report)
    } else {
        None
    };

    Output {
        bucket: params.input_data.bucket.clone(),
        key: format!("worker-{}", worker),
        timestamps,
        results: results_report,
    }
}

/// Action entry point invoked by the Burst harness.
///
/// Deserialises the JSON `Input`, validates the `partitions / granularity`
/// constraint, then delegates to `label_propagation` and re-serialises its
/// `Output`.
///
/// # Panics
/// If `partitions % granularity != 0`, or if any inner panic surfaces from
/// the propagation core (e.g. S3 fetch failure, partition/burst mismatch).
pub fn main(args: Value, burst_middleware: Middleware<LabelsMessage>) -> Result<Value, Error> {
    let input: Input = serde_json::from_value(args)?;

    assert!(
        input.partitions % input.granularity == 0,
        "partitions ({}) must be divisible by granularity ({}) for balanced distribution",
        input.partitions,
        input.granularity
    );

    let handle = burst_middleware.get_actor_handle();
    let result = label_propagation(input, &handle);
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

    /// Stand-in for the cross-host RPC proxies. Two-worker tests share a
    /// single in-process `BurstMiddleware` so remote send/recv must never be
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
            panic!("remote recv should not be used in the local distributed LP test");
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
            panic!("remote broadcast recv should not be used in the local distributed LP test");
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

    #[test]
    fn empty_partition_body_yields_no_edges() {
        let mut edges = Vec::new();
        let mut labels = Vec::new();
        parse_partition_body(1, "graphs/part-00002", "", 10, &mut edges, &mut labels);
        assert!(edges.is_empty());
        assert!(labels.is_empty());
    }

    #[test]
    fn duplicate_seed_pair_keeps_smallest_label() {
        let mut labels = vec![UNKNOWN; 4];
        apply_seed_pairs(&mut labels, &[2, 50, 2, 30, 0, 7]);
        assert_eq!(labels[0], 7);
        assert_eq!(labels[2], 30);
        assert_eq!(labels[1], UNKNOWN);
        assert_eq!(labels[3], UNKNOWN);
    }

    #[test]
    fn dedup_seed_pairs_keeps_smallest_label_and_sorts() {
        let deduped = dedup_seed_pairs(vec![(4, 90), (2, 50), (4, 70), (2, 60)]);
        assert_eq!(deduped, vec![(2, 50), (4, 70)]);
    }

    #[test]
    #[should_panic(expected = "seed node 5 out of range")]
    fn apply_seed_pairs_panics_on_out_of_range_node() {
        let mut labels = vec![UNKNOWN; 3];
        apply_seed_pairs(&mut labels, &[5, 1]);
    }

    #[test]
    #[should_panic(expected = "seed payload must contain node/label pairs")]
    fn apply_seed_pairs_panics_on_odd_payload() {
        let mut labels = vec![UNKNOWN; 3];
        apply_seed_pairs(&mut labels, &[0, 1, 2]);
    }

    #[test]
    fn merge_picks_min_when_both_sides_have_labels() {
        let left = LabelsMessage(vec![UNKNOWN, 5, 7, UNKNOWN, 3]);
        let right = LabelsMessage(vec![2, UNKNOWN, 4, 9, 11]);
        let out = merge_label_updates(left, right);
        assert_eq!(out.0, vec![2, 5, 4, 9, 14]);
    }

    #[test]
    fn merge_count_saturates() {
        let left = LabelsMessage(vec![UNKNOWN, u32::MAX - 5]);
        let right = LabelsMessage(vec![UNKNOWN, 100]);
        let out = merge_label_updates(left, right);
        assert_eq!(out.0[1], u32::MAX);
    }

    #[test]
    fn majority_label_returns_current_when_counts_empty() {
        let mut counts = HashMap::default();
        assert_eq!(majority_label(&mut counts, 42), 42);
    }

    #[test]
    fn majority_label_breaks_tie_with_smallest_label() {
        let mut counts = HashMap::default();
        counts.insert(7, 2);
        counts.insert(3, 2);
        counts.insert(5, 2);
        assert_eq!(majority_label(&mut counts, 99), 3);
    }

    #[test]
    fn majority_label_ignores_unknown_entries() {
        let mut counts = HashMap::default();
        counts.insert(UNKNOWN, 100);
        counts.insert(8, 1);
        assert_eq!(majority_label(&mut counts, 0), 8);
    }

    #[test]
    fn majority_label_clears_counts_after_use() {
        let mut counts = HashMap::default();
        counts.insert(1, 3);
        counts.insert(2, 1);
        majority_label(&mut counts, 0);
        assert!(counts.is_empty());
    }

    #[test]
    fn build_csr_graph_handles_unsorted_edges_and_isolated_nodes() {
        let edges = vec![(2, 0), (0, 1), (2, 1), (0, 2)];
        let g = build_csr_graph(4, edges);
        assert_eq!(g.owned_nodes, vec![0, 2]);
        assert_eq!(g.offsets[0..5], [0, 2, 2, 4, 4]);
        let neigh0: &[u32] = &g.flat_edges[g.offsets[0] as usize..g.offsets[1] as usize];
        let neigh2: &[u32] = &g.flat_edges[g.offsets[2] as usize..g.offsets[3] as usize];
        let mut n0 = neigh0.to_vec();
        let mut n2 = neigh2.to_vec();
        n0.sort();
        n2.sort();
        assert_eq!(n0, vec![1, 2]);
        assert_eq!(n2, vec![0, 1]);
    }

    #[test]
    fn build_csr_graph_empty_edges() {
        let g = build_csr_graph(3, vec![]);
        assert!(g.owned_nodes.is_empty());
        assert!(g.flat_edges.is_empty());
        assert_eq!(g.offsets, vec![0, 0, 0, 0]);
    }

    #[test]
    fn partition_cache_hit_and_prefix_eviction() {
        let g = build_csr_graph(2, vec![(0, 1)]);
        let entry = Arc::new((g, vec![(0u32, 7u32)]));
        partition_cache_store("dsA/part-00000".to_string(), entry);
        let hit = partition_cache_lookup("dsA/part-00000", "dsA").expect("expected cache hit");
        assert_eq!(hit.1, vec![(0, 7)], "cached seeds must round-trip");
        // A lookup under a different dataset prefix evicts the stale entry.
        assert!(partition_cache_lookup("dsB/part-00000", "dsB").is_none());
        assert!(partition_cache_lookup("dsA/part-00000", "dsA").is_none());
    }

    #[test]
    fn parse_partition_body_skips_invalid_and_negative_labels() {
        let body = "0\t1\t5\n1\t99\t3\nbad line\n2\t0\t-1\n3\t1\n";
        let mut edges = Vec::new();
        let mut labels = Vec::new();
        parse_partition_body(0, "p", body, 4, &mut edges, &mut labels);
        // valid edges: (0,1), (2,0), (3,1). (1,99) skipped (out of range).
        assert_eq!(edges, vec![(0, 1), (2, 0), (3, 1)]);
        // labels: 5 for node 0; node 2 has -1 → skipped; node 3 no label field.
        assert_eq!(labels, vec![(0, 5)]);
    }

    #[test]
    fn parse_partition_body_skips_labels_that_exceed_u32() {
        let body = "0\t1\t4294967296\n1\t2\t7\n";
        let mut edges = Vec::new();
        let mut labels = Vec::new();
        parse_partition_body(0, "p", body, 3, &mut edges, &mut labels);
        assert_eq!(edges, vec![(0, 1), (1, 2)]);
        assert_eq!(labels, vec![(1, 7)]);
    }

    #[test]
    fn should_continue_uses_default_max_iter_when_none() {
        assert!(should_continue(0, None, 5, 0));
        assert!(should_continue(MAX_ITER - 1, None, 5, 0));
        assert!(!should_continue(MAX_ITER, None, 5, 0));
    }

    #[test]
    fn should_continue_threshold_stops_loop() {
        assert!(should_continue(0, Some(10), 5, 4));
        assert!(!should_continue(0, Some(10), 4, 4));
        assert!(!should_continue(0, Some(10), 0, 0));
    }

    #[test]
    #[should_panic(expected = "not divisible by 4")]
    fn labels_message_from_bytes_rejects_odd_byte_length() {
        let _ = LabelsMessage::from(Bytes::from(vec![0u8, 1, 2]));
    }

    #[test]
    fn results_report_uses_completed_iteration_count() {
        let report = build_results_report(&[0, 1, 1, 2], 4, 1);
        assert!(report.contains("Total iterations: 1"));
    }

    /// Build a minimal [`Input`] suitable for the in-process two-worker
    /// distributed tests. S3 fields are unused because tests bypass
    /// [`load_partition_flat`] and supply pre-built CSR graphs directly.
    fn make_test_params(num_nodes: u32, max_iterations: u32) -> Input {
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
            max_iterations: Some(max_iterations),
            convergence_threshold: Some(0),
            partitions: 2,
            granularity: 1,
            group_id: None,
            timeout_seconds: None,
            use_cache: Some(false),
        }
    }

    /// Spin up a 2-worker `BurstMiddleware` in a multi-threaded Tokio runtime,
    /// hand each worker the supplied CSR graph + seed list, run the
    /// propagation core to completion, and return their `(labels, iterations)`
    /// pairs. Used by the distributed-mode integration tests below.
    fn run_two_worker_lp(
        burst_id: &str,
        params: Input,
        worker0: (CSRGraph, Vec<(u32, u32)>),
        worker1: (CSRGraph, Vec<(u32, u32)>),
    ) -> ((Vec<u32>, u32), (Vec<u32>, u32)) {
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
            .collect::<StdHashMap<u32, Middleware<LabelsMessage>>>();

        let handle0 = actors.remove(&0).unwrap().get_actor_handle();
        let handle1 = actors.remove(&1).unwrap().get_actor_handle();
        let params0 = params.clone();
        let params1 = params;
        let (g0, s0) = worker0;
        let (g1, s1) = worker1;
        let thread0 = thread::spawn(move || {
            run_label_propagation_core(&params0, &handle0, &g0, &s0, vec![timestamp("ws")])
        });
        let thread1 = thread::spawn(move || {
            run_label_propagation_core(&params1, &handle1, &g1, &s1, vec![timestamp("ws")])
        });
        let (_, labels0, iters0) = thread0.join().unwrap();
        let (_, labels1, iters1) = thread1.join().unwrap();
        ((labels0, iters0), (labels1, iters1))
    }

    #[test]
    fn distributed_lp_converges_to_expected_labels() {
        let params = make_test_params(3, 10);
        let g0 = build_csr_graph(3, vec![(0, 1), (0, 2), (2, 0), (2, 1)]);
        let g1 = build_csr_graph(3, vec![(1, 0), (1, 2)]);
        let ((labels0, iters0), (labels1, iters1)) =
            run_two_worker_lp("lp-supervised", params, (g0, vec![(0, 100)]), (g1, vec![]));
        assert_eq!(iters0, 2);
        assert_eq!(iters1, 2);
        assert_eq!(labels0, vec![100, 100, 100]);
        assert_eq!(labels1, vec![100, 100, 100]);
    }

    #[test]
    fn distributed_lp_unsupervised_converges_to_smallest_id() {
        // Connected triangle, no seeds → unsupervised init labels = node ids.
        // Tie-breaking favors smallest, so all converge to 0.
        let params = make_test_params(3, 10);
        let g0 = build_csr_graph(3, vec![(0, 1), (0, 2), (2, 0), (2, 1)]);
        let g1 = build_csr_graph(3, vec![(1, 0), (1, 2)]);
        let ((labels0, _), (labels1, _)) =
            run_two_worker_lp("lp-unsup", params, (g0, vec![]), (g1, vec![]));
        assert_eq!(labels0, vec![0, 0, 0]);
        assert_eq!(labels1, vec![0, 0, 0]);
    }

    #[test]
    fn distributed_lp_conflicting_seeds_resolved_by_min() {
        // Both workers seed node 0 with different labels (50 and 30).
        // apply_seed_pairs must keep the smaller (30); seeds are pinned, so node 0 stays 30
        // and propagates through the connected graph.
        let params = make_test_params(3, 10);
        let g0 = build_csr_graph(3, vec![(0, 1), (0, 2), (2, 0), (2, 1)]);
        let g1 = build_csr_graph(3, vec![(1, 0), (1, 2)]);
        let ((labels0, _), (labels1, _)) = run_two_worker_lp(
            "lp-conflict",
            params,
            (g0, vec![(0, 50)]),
            (g1, vec![(0, 30)]),
        );
        assert_eq!(labels0[0], 30);
        assert_eq!(labels1[0], 30);
        assert_eq!(labels0, vec![30, 30, 30]);
        assert_eq!(labels1, vec![30, 30, 30]);
    }

    #[test]
    fn distributed_lp_respects_max_iter_cap() {
        // Worker0 owns nodes 0,1 with seeds 100/200; worker1 owns nothing.
        // Both seeds are pinned and there are no other non-seed owned nodes,
        // so iter 0 produces `changed = 0` and the loop should stop after one
        // iteration regardless of the cap.
        let params = make_test_params(3, 3);
        let g0 = build_csr_graph(3, vec![(0, 1), (1, 0)]);
        let g1 = build_csr_graph(3, vec![]);
        let ((labels0, iters0), (_, iters1)) = run_two_worker_lp(
            "lp-cap",
            params,
            (g0, vec![(0, 100), (1, 200)]),
            (g1, vec![]),
        );
        assert!(iters0 <= 3, "must respect max_iterations cap");
        assert_eq!(iters0, iters1);
        assert_eq!(labels0[0], 100);
        assert_eq!(labels0[1], 200);
    }
}
