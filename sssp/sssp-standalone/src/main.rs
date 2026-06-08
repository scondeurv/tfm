//! SSSP standalone benchmark binary.
//!
//! Reads a weighted graph file (TSV: `src\tdst\tweight`), runs Bellman-Ford
//! from a source node, and prints JSON timing results compatible
//! with `benchmark_sssp.py`.
//!
//! Usage:
//!   sssp-standalone <graph_file> <num_nodes> [source_node] [max_iterations]
//!
//! Output JSON:
//!   { load_time_ms, execution_time_ms, total_time_ms, reachable_nodes, max_distance, distances }

use sssp_standalone::{run_bellman_ford, MAX_ITERATIONS};
use std::env;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::time::Instant;

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 3 {
        eprintln!(
            "Usage: {} <graph_file> <num_nodes> [source_node] [max_iterations]",
            args[0]
        );
        std::process::exit(1);
    }

    let graph_file = &args[1];
    let num_nodes: u32 = args[2].parse().expect("Invalid num_nodes");
    let source_node: u32 = args.get(3).and_then(|s| s.parse().ok()).unwrap_or(0);
    let max_iterations: u32 = args
        .get(4)
        .and_then(|s| s.parse().ok())
        .unwrap_or(MAX_ITERATIONS);

    let start_load = Instant::now();
    let adj = load_graph(graph_file, num_nodes);
    let load_duration = start_load.elapsed();

    let start_algo = Instant::now();
    let result = run_bellman_ford(&adj, source_node, num_nodes, max_iterations);
    let algo_duration = start_algo.elapsed();

    // Output JSON – same schema as other standalone binaries for benchmark compatibility
    let output = serde_json::json!({
        "load_time_ms":      load_duration.as_millis(),
        "execution_time_ms": algo_duration.as_millis(),
        "total_time_ms":     (load_duration + algo_duration).as_millis(),
        "reachable_nodes":   result.reachable_nodes,
        "max_distance":      result.max_distance,
        "distances":         result.distances
    });

    println!("{}", output);
}

/// Load a directed weighted graph from a TSV file (`src\tdst\tweight`).
///
/// If weight column is missing, defaults to 1.0.
fn load_graph(path: &str, num_nodes: u32) -> Vec<Vec<(u32, f32)>> {
    let file = File::open(path).expect("Could not open graph file");
    let reader = BufReader::new(file);
    let mut adj: Vec<Vec<(u32, f32)>> = vec![Vec::new(); num_nodes as usize];

    for line in reader.lines() {
        let line = line.expect("Could not read line");
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
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
        let weight: f32 = parts
            .next()
            .and_then(|s| s.parse().ok())
            .unwrap_or(1.0);
        assert!(
            weight >= 0.0,
            "Negative edge weights are unsupported for this SSSP benchmark"
        );

        if src < num_nodes && dst < num_nodes {
            adj[src as usize].push((dst, weight));
        }
    }

    adj
}
