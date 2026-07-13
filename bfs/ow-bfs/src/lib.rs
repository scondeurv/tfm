//! Distributed Breadth-First Search (BFS) action.
//!
//! # Overview
//!
//! Implements level-synchronous BFS across a fleet of Burst workers. Each
//! worker:
//!
//! 1. Fetches its assigned graph partition from S3.
//! 2. Builds a local Compressed-Sparse-Row (CSR) representation.
//! 3. At every level, expands the current global frontier along the
//!    out-edges of nodes it owns and ships back only the newly-reached
//!    `(node, level)` pairs.
//! 4. Synchronises with peers via `reduce` + `broadcast` to merge the
//!    per-worker discoveries into the next global frontier.
//! 5. Stops when the merged frontier is empty or `max_levels` is reached.
//!
//! Worker `0` (the `ROOT_WORKER`) writes the final per-node level vector
//! to S3 and emits a human-readable summary.
//!
//! # Partition model
//!
//! - Exactly one global partition per worker (`partitions == burst_size`).
//! - Every node id has a single owning worker; non-owners contribute no
//!   discoveries. Duplicate discoveries from overlapping ownership are
//!   resolved by smallest level in `merge_frontiers`.
//!
//! # Wire format
//!
//! Inter-worker traffic uses [`FrontierMessage`], a sparse list of
//! `(node, level)` pairs. Per-iteration cost is `O(frontier_size)` rather
//! than `O(num_nodes)`.

use ahash::AHashMap as HashMap;
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

/// Worker that hosts the canonical aggregation role: roots the reduce/broadcast
/// collectives and writes the final S3 output.
const ROOT_WORKER: u32 = 0;

/// Default cap on BFS levels when the input does not specify one.
const MAX_LEVELS: u32 = 500;

/// Sentinel marking "node not yet reached by BFS" inside the level vector.
const UNVISITED: u32 = u32::MAX;

/// JSON-serialisable input the action receives from the Burst harness.
///
/// Field invariants:
/// - `partitions` must equal `burst_size` (one partition per worker).
/// - `partitions` and `granularity` must be non-zero powers of two.
/// - `partitions % granularity == 0` for balanced Burst pack placement.
/// - `source_node` defaults to `0`; `max_levels` defaults to [`MAX_LEVELS`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Input {
    /// S3 location and credentials for the partition shards.
    input_data: S3InputParams,
    /// Total number of vertices in the global graph.
    num_nodes: u32,
    /// BFS source vertex. Defaults to `0` if absent.
    source_node: Option<u32>,
    /// Cap on BFS levels. Defaults to [`MAX_LEVELS`].
    max_levels: Option<u32>,
    /// Number of partitions the graph was sharded into. Equals `burst_size`.
    partitions: u32,
    /// Workers per Burst pack/group. `partitions % granularity == 0`.
    granularity: u32,
    /// Enable the in-memory burst-local partition cache (defaults to `true`).
    /// Set to `false` to force the S3 fetch + parse + CSR build on every
    /// invocation, reproducing the un-cached deployment behaviour.
    #[serde(default)]
    use_cache: Option<bool>,
}

/// S3 endpoint + credentials + key prefix where partition shards live.
///
/// Each shard is fetched from `{bucket}/{key}/part-{worker_id:05}`. When
/// `endpoint` is set (non-AWS S3-compatible stores like MinIO), path-style
/// addressing is forced.
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

/// Sparse frontier message exchanged between workers each BFS level.
///
/// Wraps a `Vec<(u32, u32)>` of `(node, level)` pairs newly discovered at
/// this level. Wire encoding is the flat little-endian sequence
/// `[node_0, level_0, node_1, level_1, …]`, so the message size is
/// `2 · entries · 4` bytes — proportional to the live frontier rather than
/// to `num_nodes`. An empty `Vec` signals convergence.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct FrontierMessage(pub Vec<(u32, u32)>);

impl From<Bytes> for FrontierMessage {
    /// Decode the little-endian wire format. Trailing bytes that do not
    /// form a complete `(node, level)` pair are silently dropped.
    fn from(bytes: Bytes) -> Self {
        let data = bytes.as_ref();
        let num_pairs = data.len() / 8;
        let mut entries = Vec::with_capacity(num_pairs);
        for i in 0..num_pairs {
            let off = i * 8;
            let node = u32::from_le_bytes(data[off..off + 4].try_into().unwrap());
            let level = u32::from_le_bytes(data[off + 4..off + 8].try_into().unwrap());
            entries.push((node, level));
        }
        FrontierMessage(entries)
    }
}

impl From<FrontierMessage> for Bytes {
    /// Encode the message into the flat little-endian wire format described
    /// on [`FrontierMessage`].
    fn from(val: FrontierMessage) -> Self {
        let mut buf = Vec::with_capacity(val.0.len() * 8 + 1);
        for &(node, level) in &val.0 {
            buf.extend_from_slice(&node.to_le_bytes());
            buf.extend_from_slice(&level.to_le_bytes());
        }
        if buf.is_empty() {
            // Burst middleware drops zero-byte payloads; emit a single padding
            // byte so empty frontiers still travel. The decoder ignores any
            // trailing bytes that do not form a complete pair.
            buf.push(0);
        }
        Bytes::from(buf)
    }
}

/// Compressed-Sparse-Row representation of the per-worker subgraph.
///
/// - `owned_nodes` lists the source nodes the worker actually has out-edges
///   for; nodes absent from this list have an empty range in `offsets` and
///   contribute nothing to the frontier expansion.
/// - `offsets[i]` is the start of node `i`'s adjacency list in `flat_edges`,
///   `offsets[i+1]` the end. `offsets` has length `num_nodes + 1`.
/// - `flat_edges` is the concatenation of every owned node's out-neighbours.
struct CSRGraph {
    owned_nodes: Vec<u32>,
    offsets: Vec<u32>,
    flat_edges: Vec<u32>,
}

/// Parse one partition's tab-separated body into an edge list.
///
/// Each non-blank line has the form `src \t dst [\t label]`. The optional
/// third column is a holdover from the LP partition format and is ignored
/// by BFS. Lines that fail to parse, or whose endpoints fall outside
/// `[0, num_nodes)`, are skipped with a warning written to `stderr`.
fn parse_partition_body(
    worker_id: u32,
    part_key: &str,
    body: &str,
    num_nodes: u32,
    edges: &mut Vec<(u32, u32)>,
) {
    for (line_idx, line) in body.lines().enumerate() {
        if line.trim().is_empty() {
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
        } else {
            eprintln!(
                "[Worker {worker_id}] Malformed edge line in {part_key}:{}: {line:?}",
                line_idx + 1
            );
        }
    }
}

/// Build a [`CSRGraph`] from an unsorted edge list.
///
/// Edges are sorted by source so neighbours of node `i` end up contiguous
/// in `flat_edges` between `offsets[i]` and `offsets[i + 1]`. Nodes without
/// any out-edge are absent from `owned_nodes` and have an empty range.
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
        graph.flat_edges.len()
    );
    if graph.flat_edges.is_empty() {
        // Empty partitions are valid: BFS still produces a result (typically
        // only the source visited) and the worker contributes nothing to the
        // global frontier.
        println!("[Worker {worker_id}] Warning: empty partition; continuing");
    }
    Ok(graph)
}

/// Merge two per-worker frontier messages into one.
///
/// Concatenates entries, then dedup-sorts by `(node, level)` ascending and
/// keeps the first occurrence of each node — which, after the lexicographic
/// sort, is the smallest level. The result is therefore deterministic and
/// tolerant of overlapping ownership (a node discovered by multiple workers
/// at different levels collapses to its earliest level).
fn merge_frontiers(mut left: FrontierMessage, right: FrontierMessage) -> FrontierMessage {
    left.0.extend(right.0);
    left.0
        .sort_unstable_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));
    let mut write = 0usize;
    for read in 0..left.0.len() {
        if write == 0 || left.0[read].0 != left.0[write - 1].0 {
            left.0[write] = left.0[read];
            write += 1;
        }
    }
    left.0.truncate(write);
    left
}

/// Render the final BFS state into a human-readable summary used both for
/// stdout logging and for the `results` field returned by [`ROOT_WORKER`].
fn build_results_report(
    levels: &[u32],
    num_nodes: u32,
    source: u32,
    total_visited: u64,
    max_level: u32,
    executed_iters: u32,
) -> String {
    let mut report = String::new();
    report.push_str("\n=== BFS Results ===\n");
    let _ = writeln!(report, "Source node:    {source}");
    let _ = writeln!(report, "Total nodes:    {num_nodes}");
    let _ = writeln!(report, "Visited nodes:  {total_visited}");
    let _ = writeln!(report, "Max BFS level:  {max_level}");
    let _ = writeln!(report, "BFS levels run: {executed_iters}");

    let mut level_counts: HashMap<u32, usize> = HashMap::default();
    for &lv in levels {
        *level_counts.entry(lv).or_insert(0) += 1;
    }
    report.push_str("\nLevel distribution (first 20):\n");
    let mut sorted: Vec<_> = level_counts.iter().collect();
    sorted.sort_by_key(|&(&lv, _)| lv);
    for (&lv, &cnt) in sorted.iter().take(20) {
        if lv == UNVISITED {
            let _ = writeln!(report, "  UNVISITED: {cnt} nodes");
        } else {
            let _ = writeln!(report, "  Level {lv:3}: {cnt} nodes");
        }
    }
    report.push_str("===================\n");
    report
}

/// Core distributed BFS loop, decoupled from S3 I/O.
///
/// On entry every worker has its local CSR graph and the same
/// `params`. The function:
///
/// 1. Seeds `levels[source] = 0` and the local frontier with the source
///    (when the source falls inside `[0, num_nodes)`).
/// 2. Loops: each owned source in the current frontier expands its
///    out-edges into a local discovery map, the local discoveries are
///    reduced via [`merge_frontiers`] and broadcast back as the next
///    global frontier.
/// 3. Stops when the merged frontier is empty or `max_levels` is reached.
///
/// Returns `(timestamps, levels, executed_iters, max_level, total_visited)`
/// where `executed_iters` is the number of iterations that actually expanded
/// a non-empty frontier.
///
/// # Panics
/// - If `params.partitions != burst_size` (broken partition model).
/// - If `worker_id >= burst_size`.
fn run_bfs_core(
    params: &Input,
    middleware: &MiddlewareActorHandle<FrontierMessage>,
    graph: &CSRGraph,
    mut timestamps: Vec<Timestamp>,
) -> (Vec<Timestamp>, Vec<u32>, u32, u32, u64) {
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
    let max_levels_val = params.max_levels.unwrap_or(MAX_LEVELS);
    let n = params.num_nodes as usize;

    println!(
        "[Worker {worker}] starting BFS (burst_size={burst_size}, num_nodes={}, source={source})",
        params.num_nodes
    );

    let mut levels = vec![UNVISITED; n];
    let source_in_graph = source < params.num_nodes;
    if source_in_graph {
        levels[source as usize] = 0;
    }

    let mut current_frontier: Vec<(u32, u32)> = if source_in_graph {
        vec![(source, 0)]
    } else {
        Vec::new()
    };

    let mut executed_iters: u32 = 0;
    let mut total_visited: u64 = u64::from(source_in_graph);
    let mut max_level: u32 = 0;

    while executed_iters < max_levels_val {
        if current_frontier.is_empty() {
            break;
        }
        timestamps.push(timestamp(&format!("iter_{executed_iters}_start")));

        // Nodes not in `owned_nodes` have an empty range in `offsets`, so
        // the inner loop body is skipped automatically — no explicit
        // ownership check is needed.
        let mut local_discovered: HashMap<u32, u32> = HashMap::default();
        for &(src_node, src_level) in &current_frontier {
            let src_idx = src_node as usize;
            if src_idx >= n {
                continue;
            }
            let next_level = src_level.saturating_add(1);
            let start = graph.offsets[src_idx] as usize;
            let end = graph.offsets[src_idx + 1] as usize;
            for i in start..end {
                let dst = graph.flat_edges[i] as usize;
                if dst < n && levels[dst] == UNVISITED {
                    let entry = local_discovered.entry(dst as u32).or_insert(next_level);
                    if next_level < *entry {
                        *entry = next_level;
                    }
                }
            }
        }

        let mut frontier_entries: Vec<(u32, u32)> = local_discovered.into_iter().collect();
        frontier_entries.sort_unstable_by_key(|&(node, _)| node);

        timestamps.push(timestamp(&format!("iter_{executed_iters}_compute")));

        let reduced = middleware
            .reduce(FrontierMessage(frontier_entries), merge_frontiers)
            .unwrap();
        timestamps.push(timestamp(&format!("iter_{executed_iters}_reduce")));

        let global = middleware.broadcast(reduced, ROOT_WORKER).unwrap();
        timestamps.push(timestamp(&format!("iter_{executed_iters}_broadcast")));

        let mut newly_reached = 0u32;
        for &(node, level) in &global.0 {
            let idx = node as usize;
            if idx < n {
                if levels[idx] == UNVISITED {
                    levels[idx] = level;
                    newly_reached += 1;
                    if level > max_level {
                        max_level = level;
                    }
                } else if level < levels[idx] {
                    levels[idx] = level;
                    max_level = levels
                        .iter()
                        .copied()
                        .filter(|&lv| lv != UNVISITED)
                        .max()
                        .unwrap_or(0);
                }
            }
        }

        total_visited += u64::from(newly_reached);

        if worker == ROOT_WORKER {
            println!(
                "[Worker {worker}] BFS level {executed_iters}: newly_reached={newly_reached}, total_visited={total_visited} (frontier_bytes={})",
                global.0.len() * 8
            );
        }

        let global_is_empty = global.0.is_empty();
        current_frontier = global.0;

        if global_is_empty {
            break;
        }
        executed_iters += 1;
    }

    if executed_iters >= max_levels_val && !current_frontier.is_empty() {
        eprintln!(
            "[Worker {worker}] WARNING: BFS reached MAX_LEVELS={max_levels_val} \
             with non-empty frontier ({} entries) — distant nodes may be missing",
            current_frontier.len()
        );
    }

    timestamps.push(timestamp("worker_end"));
    (timestamps, levels, executed_iters, max_level, total_visited)
}

/// Per-worker entry point: build the runtime, fetch the partition, run the
/// BFS core, and (on [`ROOT_WORKER`]) write the level vector to S3 plus the
/// human-readable summary.
fn bfs(params: Input, middleware: &MiddlewareActorHandle<FrontierMessage>) -> Output {
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

    let (mut timestamps, levels, executed_iters, max_level, total_visited) =
        run_bfs_core(&params, middleware, &graph, timestamps);

    let source = params.source_node.unwrap_or(0);
    let results_report = if worker == ROOT_WORKER {
        timestamps.push(timestamp("write_output_start"));

        let report = build_results_report(
            &levels,
            params.num_nodes,
            source,
            total_visited,
            max_level,
            executed_iters,
        );
        println!("{report}");

        if params.num_nodes < 10_000_000 {
            let output_key = format!("{}/output/bfs_levels_final.json", params.input_data.key);
            let levels_json = serde_json::json!({
                "source": source,
                "visited_nodes": total_visited,
                "max_level": max_level,
                "levels": levels,
            });
            let levels_str = serde_json::to_string(&levels_json).unwrap();
            let write_result = rt.block_on(async {
                s3_client
                    .put_object()
                    .bucket(&params.input_data.bucket)
                    .key(&output_key)
                    .body(levels_str.into_bytes().into())
                    .send()
                    .await
            });
            match write_result {
                Ok(_) => println!(
                    "[Worker {worker}] ✓ Wrote BFS levels to s3://{}/{output_key}",
                    params.input_data.bucket
                ),
                Err(e) => eprintln!("[Worker {worker}] ✗ Failed to write levels: {e:?}"),
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
/// Deserialises the JSON `Input`, validates the Burst shape constraints,
/// then delegates to `bfs` and re-serialises its `Output`.
///
/// # Errors
/// Returns `Err` if the input JSON cannot be deserialised into `Input` or
/// if the `Output` cannot be re-serialised back to JSON.
///
/// # Panics
/// If the partition/granularity contract is invalid, or if any inner panic
/// surfaces from the BFS core (e.g. S3 fetch failure, partition/burst mismatch).
fn validate_input_contract(input: &Input) {
    assert!(input.partitions > 0, "partitions must be greater than zero");
    assert!(
        input.granularity > 0,
        "granularity must be greater than zero"
    );
    assert!(
        input.granularity <= input.partitions,
        "granularity ({}) must be <= partitions ({})",
        input.granularity,
        input.partitions
    );
    assert!(
        input.partitions % input.granularity == 0,
        "partitions ({}) must be divisible by granularity ({})",
        input.partitions,
        input.granularity
    );
    assert!(
        input.partitions.is_power_of_two(),
        "partitions ({}) must be a power of two to match Burst reduce",
        input.partitions
    );
    assert!(
        input.granularity.is_power_of_two(),
        "granularity ({}) must be a power of two to match Burst group collectives",
        input.granularity
    );
}

pub fn main(args: Value, burst_middleware: Middleware<FrontierMessage>) -> Result<Value, Error> {
    let input: Input = serde_json::from_value(args)?;
    validate_input_contract(&input);
    let handle = burst_middleware.get_actor_handle();
    let result = bfs(input, &handle);
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
            panic!("remote recv should not be used in the local distributed BFS test");
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
            panic!("remote broadcast recv should not be used in the local distributed BFS test");
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
    fn parse_partition_body_skips_invalid() {
        let body = "0\t1\n1\t99\nbad line\n2\t0\t-1\n3\t1\n";
        let mut edges = Vec::new();
        parse_partition_body(0, "p", body, 4, &mut edges);
        // (1,99) skipped (out of range); the trailing label column on (2,0)
        // is ignored by BFS but the edge itself is still accepted.
        assert_eq!(edges, vec![(0, 1), (2, 0), (3, 1)]);
    }

    #[test]
    fn parse_partition_body_empty_yields_no_edges() {
        let mut edges = Vec::new();
        parse_partition_body(1, "p", "", 5, &mut edges);
        assert!(edges.is_empty());
    }

    #[test]
    fn build_csr_graph_handles_unsorted_and_isolated() {
        let g = build_csr_graph(4, vec![(2, 0), (0, 1), (2, 1), (0, 2)]);
        assert_eq!(g.owned_nodes, vec![0, 2]);
        assert_eq!(g.offsets, vec![0, 2, 2, 4, 4]);
        let mut n0 = g.flat_edges[0..2].to_vec();
        let mut n2 = g.flat_edges[2..4].to_vec();
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
        let g = Arc::new(build_csr_graph(2, vec![(0, 1)]));
        partition_cache_store("dsA/part-00000".to_string(), g);
        assert!(partition_cache_lookup("dsA/part-00000", "dsA").is_some());
        // A lookup under a different dataset prefix evicts the stale entry.
        assert!(partition_cache_lookup("dsB/part-00000", "dsB").is_none());
        assert!(partition_cache_lookup("dsA/part-00000", "dsA").is_none());
    }

    #[test]
    fn frontier_message_roundtrips_through_bytes() {
        let msg = FrontierMessage(vec![(7, 1), (12, 1), (200, 2)]);
        let bytes: Bytes = msg.clone().into();
        let back: FrontierMessage = bytes.into();
        assert_eq!(back, msg);
    }

    #[test]
    fn frontier_message_empty_roundtrip() {
        let msg = FrontierMessage(vec![]);
        let bytes: Bytes = msg.clone().into();
        assert_eq!(bytes.len(), 1);
        let back: FrontierMessage = bytes.into();
        assert_eq!(back, msg);
    }

    #[test]
    fn frontier_message_truncates_partial_trailing_pair() {
        // One full pair (8 bytes) + 3 stray bytes.
        let mut buf = vec![];
        buf.extend_from_slice(&5u32.to_le_bytes());
        buf.extend_from_slice(&9u32.to_le_bytes());
        buf.extend_from_slice(&[0xAA, 0xBB, 0xCC]);
        let msg: FrontierMessage = Bytes::from(buf).into();
        assert_eq!(msg.0, vec![(5, 9)]);
    }

    #[test]
    fn merge_frontiers_keeps_smallest_level_per_node() {
        let left = FrontierMessage(vec![(1, 3), (4, 2)]);
        let right = FrontierMessage(vec![(1, 1), (2, 5), (4, 7)]);
        let out = merge_frontiers(left, right);
        assert_eq!(out.0, vec![(1, 1), (2, 5), (4, 2)]);
    }

    #[test]
    fn merge_frontiers_disjoint_entries_concatenate() {
        let left = FrontierMessage(vec![(1, 1)]);
        let right = FrontierMessage(vec![(2, 1), (3, 1)]);
        let out = merge_frontiers(left, right);
        assert_eq!(out.0, vec![(1, 1), (2, 1), (3, 1)]);
    }

    #[test]
    fn merge_frontiers_empty_sides() {
        let left = FrontierMessage(vec![]);
        let right = FrontierMessage(vec![(2, 1)]);
        let out = merge_frontiers(left, right);
        assert_eq!(out.0, vec![(2, 1)]);
    }

    #[test]
    fn build_results_report_includes_key_stats() {
        let levels = vec![0, 1, 1, 2];
        let report = build_results_report(&levels, 4, 0, 4, 2, 3);
        assert!(report.contains("Source node:    0"));
        assert!(report.contains("Total nodes:    4"));
        assert!(report.contains("Visited nodes:  4"));
        assert!(report.contains("Max BFS level:  2"));
        assert!(report.contains("BFS levels run: 3"));
    }

    fn make_test_params(num_nodes: u32, source: u32, max_levels: u32) -> Input {
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
            max_levels: Some(max_levels),
            partitions: 2,
            granularity: 1,
            use_cache: Some(false),
        }
    }

    #[test]
    fn validate_input_contract_accepts_power_of_two_campaign_shape() {
        let mut params = make_test_params(4, 0, 100);
        params.partitions = 16;
        params.granularity = 8;
        validate_input_contract(&params);
    }

    #[test]
    fn validate_input_contract_rejects_zero_granularity() {
        let mut params = make_test_params(4, 0, 100);
        params.granularity = 0;
        let result = std::panic::catch_unwind(|| validate_input_contract(&params));
        assert!(result.is_err(), "expected zero granularity to panic");
    }

    #[test]
    fn validate_input_contract_rejects_non_power_of_two_partitions() {
        let mut params = make_test_params(4, 0, 100);
        params.partitions = 6;
        params.granularity = 2;
        let result = std::panic::catch_unwind(|| validate_input_contract(&params));
        assert!(
            result.is_err(),
            "expected non-power-of-two partitions to panic"
        );
    }

    /// Spin up a 2-worker `BurstMiddleware` in a multi-threaded Tokio runtime,
    /// hand each worker its CSR graph, run [`run_bfs_core`] to completion,
    /// and return both workers' `(levels, executed_iters, max_level, total_visited)`.
    fn run_two_worker_bfs(
        burst_id: &str,
        params: Input,
        worker0_graph: CSRGraph,
        worker1_graph: CSRGraph,
    ) -> ((Vec<u32>, u32, u32, u64), (Vec<u32>, u32, u32, u64)) {
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
            .collect::<StdHashMap<u32, Middleware<FrontierMessage>>>();

        let handle0 = actors.remove(&0).unwrap().get_actor_handle();
        let handle1 = actors.remove(&1).unwrap().get_actor_handle();
        let params0 = params.clone();
        let params1 = params;

        let thread0 = thread::spawn(move || {
            run_bfs_core(&params0, &handle0, &worker0_graph, vec![timestamp("ws")])
        });
        let thread1 = thread::spawn(move || {
            run_bfs_core(&params1, &handle1, &worker1_graph, vec![timestamp("ws")])
        });
        let (_, l0, e0, m0, v0) = thread0.join().unwrap();
        let (_, l1, e1, m1, v1) = thread1.join().unwrap();
        ((l0, e0, m0, v0), (l1, e1, m1, v1))
    }

    #[test]
    fn distributed_bfs_chain_propagates_levels() {
        // Directed chain 0 → 1 → 2 → 3. Worker0 owns 0,2; worker1 owns 1,3.
        let params = make_test_params(4, 0, 100);
        let g0 = build_csr_graph(4, vec![(0, 1), (2, 3)]);
        let g1 = build_csr_graph(4, vec![(1, 2)]);
        let ((l0, iters, max_lv, visited), (l1, _, _, _)) =
            run_two_worker_bfs("bfs-chain", params, g0, g1);
        assert_eq!(l0, vec![0, 1, 2, 3]);
        assert_eq!(l1, vec![0, 1, 2, 3]);
        assert_eq!(iters, 3);
        assert_eq!(max_lv, 3);
        assert_eq!(visited, 4);
    }

    #[test]
    fn distributed_bfs_unreached_node_stays_unvisited() {
        // 0 → 1; node 2 has no incoming edge.
        let params = make_test_params(3, 0, 100);
        let g0 = build_csr_graph(3, vec![(0, 1)]);
        let g1 = build_csr_graph(3, vec![]);
        let ((levels, _, _, visited), _) = run_two_worker_bfs("bfs-disc", params, g0, g1);
        assert_eq!(levels[0], 0);
        assert_eq!(levels[1], 1);
        assert_eq!(levels[2], UNVISITED);
        assert_eq!(visited, 2);
    }

    #[test]
    fn distributed_bfs_overlapping_discovery_takes_min_level() {
        // Diamond 0 → {1,2}, 1 → 3, 2 → 3. Both workers can discover 3 at
        // level 2 — merge_frontiers must dedupe to a single (3,2) entry.
        let params = make_test_params(4, 0, 100);
        let g0 = build_csr_graph(4, vec![(0, 1), (0, 2), (1, 3)]);
        let g1 = build_csr_graph(4, vec![(2, 3)]);
        let ((levels, _, max_lv, visited), _) = run_two_worker_bfs("bfs-diamond", params, g0, g1);
        assert_eq!(levels, vec![0, 1, 1, 2]);
        assert_eq!(max_lv, 2);
        assert_eq!(visited, 4);
    }

    #[test]
    fn distributed_bfs_isolated_source_visits_only_itself() {
        let params = make_test_params(3, 0, 100);
        let g0 = build_csr_graph(3, vec![(1, 2)]);
        let g1 = build_csr_graph(3, vec![]);
        let ((levels, iters, max_lv, visited), _) = run_two_worker_bfs("bfs-iso", params, g0, g1);
        assert_eq!(levels[0], 0);
        assert_eq!(levels[1], UNVISITED);
        assert_eq!(levels[2], UNVISITED);
        // No new nodes ever discovered → no productive level was run.
        assert_eq!(iters, 0);
        assert_eq!(max_lv, 0);
        assert_eq!(visited, 1);
    }

    #[test]
    fn distributed_bfs_source_out_of_range_visits_nothing() {
        let params = make_test_params(3, 99, 100);
        let g0 = build_csr_graph(3, vec![(0, 1)]);
        let g1 = build_csr_graph(3, vec![]);
        let ((levels, iters, max_lv, visited), _) = run_two_worker_bfs("bfs-oor", params, g0, g1);
        assert!(levels.iter().all(|&l| l == UNVISITED));
        assert_eq!(iters, 0);
        assert_eq!(max_lv, 0);
        assert_eq!(visited, 0);
    }

    #[test]
    fn distributed_bfs_directed_orientation_blocks_reverse_edge() {
        // Edge 1 → 0; starting at 0 must not reach 1.
        let params = make_test_params(2, 0, 100);
        let g0 = build_csr_graph(2, vec![]);
        let g1 = build_csr_graph(2, vec![(1, 0)]);
        let ((levels, _, _, visited), _) = run_two_worker_bfs("bfs-dir", params, g0, g1);
        assert_eq!(levels[0], 0);
        assert_eq!(levels[1], UNVISITED);
        assert_eq!(visited, 1);
    }

    #[test]
    fn distributed_bfs_max_levels_cap_truncates_traversal() {
        // Chain 0 → 1 → 2 → 3 → 4 with cap = 2: only nodes at depth 0..=2
        // should be reached; nodes 3 and 4 stay UNVISITED.
        let params = make_test_params(5, 0, 2);
        let g0 = build_csr_graph(5, vec![(0, 1), (2, 3)]);
        let g1 = build_csr_graph(5, vec![(1, 2), (3, 4)]);
        let ((levels, iters, max_lv, visited), _) = run_two_worker_bfs("bfs-cap", params, g0, g1);
        assert_eq!(levels[0], 0);
        assert_eq!(levels[1], 1);
        assert_eq!(levels[2], 2);
        assert_eq!(levels[3], UNVISITED);
        assert_eq!(levels[4], UNVISITED);
        assert_eq!(iters, 2);
        assert_eq!(max_lv, 2);
        assert_eq!(visited, 3);
    }

    #[test]
    fn distributed_bfs_cycle_terminates_without_revisiting() {
        // 0 → 1 → 2 → 0. BFS must not loop forever; the back-edge to 0
        // is filtered by `levels[0] != UNVISITED`.
        let params = make_test_params(3, 0, 100);
        let g0 = build_csr_graph(3, vec![(0, 1), (2, 0)]);
        let g1 = build_csr_graph(3, vec![(1, 2)]);
        let ((levels, iters, max_lv, visited), _) = run_two_worker_bfs("bfs-cycle", params, g0, g1);
        assert_eq!(levels, vec![0, 1, 2]);
        assert_eq!(iters, 2);
        assert_eq!(max_lv, 2);
        assert_eq!(visited, 3);
    }

    #[test]
    fn distributed_bfs_multi_component_leaves_other_unvisited() {
        // Two disjoint components: {0 → 1} and {2 → 3}. Starting at 0
        // reaches 1 only.
        let params = make_test_params(4, 0, 100);
        let g0 = build_csr_graph(4, vec![(0, 1)]);
        let g1 = build_csr_graph(4, vec![(2, 3)]);
        let ((levels, _, _, visited), _) = run_two_worker_bfs("bfs-multi", params, g0, g1);
        assert_eq!(levels[0], 0);
        assert_eq!(levels[1], 1);
        assert_eq!(levels[2], UNVISITED);
        assert_eq!(levels[3], UNVISITED);
        assert_eq!(visited, 2);
    }

    #[test]
    fn distributed_bfs_star_high_fanout_one_iteration() {
        // 0 → {1, 2, 3, 4}: every leaf reached at level 1 in a single
        // productive iteration.
        let params = make_test_params(5, 0, 100);
        let g0 = build_csr_graph(5, vec![(0, 1), (0, 2), (0, 3), (0, 4)]);
        let g1 = build_csr_graph(5, vec![]);
        let ((levels, iters, max_lv, visited), _) = run_two_worker_bfs("bfs-star", params, g0, g1);
        assert_eq!(levels, vec![0, 1, 1, 1, 1]);
        assert_eq!(iters, 1);
        assert_eq!(max_lv, 1);
        assert_eq!(visited, 5);
    }

    #[test]
    fn distributed_bfs_self_loop_does_not_affect_result() {
        // Self-loop on the source: dst=0 already visited at level 0 so
        // the edge is skipped. The remaining 0 → 1 edge still works.
        let params = make_test_params(2, 0, 100);
        let g0 = build_csr_graph(2, vec![(0, 0), (0, 1)]);
        let g1 = build_csr_graph(2, vec![]);
        let ((levels, iters, _, visited), _) = run_two_worker_bfs("bfs-selfloop", params, g0, g1);
        assert_eq!(levels, vec![0, 1]);
        assert_eq!(iters, 1);
        assert_eq!(visited, 2);
    }

    #[test]
    fn run_bfs_core_panics_on_partition_burst_mismatch() {
        // burst_size will be 2 (two-worker harness) but partitions=99.
        // The worker thread panics with a clear message; we catch it and
        // assert on the payload because `JoinHandle::join` wraps panics.
        let mut params = make_test_params(2, 0, 100);
        params.partitions = 99;
        let g0 = build_csr_graph(2, vec![(0, 1)]);
        let result =
            std::panic::catch_unwind(|| run_two_worker_bfs("bfs-mismatch", params, g0, g1_dummy()));
        assert!(result.is_err(), "expected partition mismatch to panic");
    }

    fn g1_dummy() -> CSRGraph {
        build_csr_graph(2, vec![])
    }

    #[test]
    fn parse_partition_body_ignores_third_column() {
        // BFS partition files share the LP layout `src \t dst \t label`.
        // The label column must be ignored without rejecting the line.
        let body = "0\t1\t42\n2\t3\t-1\n";
        let mut edges = Vec::new();
        parse_partition_body(0, "p", body, 4, &mut edges);
        assert_eq!(edges, vec![(0, 1), (2, 3)]);
    }

    #[test]
    fn merge_frontiers_is_commutative_for_overlapping_inputs() {
        let a = FrontierMessage(vec![(1, 3), (4, 2)]);
        let b = FrontierMessage(vec![(1, 1), (2, 5), (4, 7)]);
        let ab = merge_frontiers(a.clone(), b.clone());
        let ba = merge_frontiers(b, a);
        assert_eq!(ab.0, ba.0);
    }
}
