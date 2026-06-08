//! BFS standalone benchmark binary.
//!
//! Reads a graph file (TSV: `src\tdst`, optional 3rd column ignored),
//! runs queue-based BFS from a source node, and prints JSON timing results
//! compatible with `benchmark_bfs.py`.
//!
//! Usage:
//!   bfs-standalone <graph_file> <num_nodes> [source_node] [max_levels]
//!
//! Output JSON:
//!   { load_time_ms, execution_time_ms, total_time_ms, visited_nodes, max_level, levels }

use bfs_standalone::run_bfs;
use std::collections::HashMap;
use std::env;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::time::Instant;

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 3 {
        eprintln!(
            "Usage: {} <graph_file> <num_nodes> [source_node] [max_levels]",
            args[0]
        );
        std::process::exit(1);
    }

    let graph_file = &args[1];
    let num_nodes: u32 = args[2].parse().expect("Invalid num_nodes");
    let source_node: u32 = args.get(3).and_then(|s| s.parse().ok()).unwrap_or(0);
    let max_levels: u32 = args
        .get(4)
        .and_then(|s| s.parse().ok())
        .unwrap_or(u32::MAX);

    let start_load = Instant::now();
    let adj = load_graph(graph_file);
    let load_duration = start_load.elapsed();

    let start_bfs = Instant::now();
    let result = run_bfs(&adj, source_node, num_nodes, max_levels);
    let bfs_duration = start_bfs.elapsed();

    // Output JSON – same schema as lpst for benchmark_bfs.py compatibility
    let output = serde_json::json!({
        "load_time_ms":      load_duration.as_millis(),
        "execution_time_ms": bfs_duration.as_millis(),
        "total_time_ms":     (load_duration + bfs_duration).as_millis(),
        "visited_nodes":     result.visited_nodes,
        "max_level":         result.max_level,
        "levels":            result.levels
    });

    println!("{}", output);
}

/// Load a directed graph from a TSV file (`src\tdst[\t<ignored>]`).
fn load_graph(path: &str) -> HashMap<u32, Vec<u32>> {
    let file = File::open(path).expect("Could not open graph file");
    let reader = BufReader::new(file);
    let mut adj: HashMap<u32, Vec<u32>> = HashMap::new();

    for line in reader.lines() {
        let line = line.expect("Could not read line");
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let mut parts = line.split('\t');
        let src: u32 = match parts.next().and_then(|s| s.parse().ok()) {
            Some(v) => v,
            None => continue,
        };
        let dst: u32 = match parts.next().and_then(|s| s.parse().ok()) {
            Some(v) => v,
            None => continue,
        };
        // Third column (label in LP graphs) is silently ignored
        adj.entry(src).or_default().push(dst);
    }

    adj
}
