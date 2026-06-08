//! Single-thread PageRank benchmark binary.
//!
//! CLI: `pagerank-standalone <graph_file> <num_nodes> <max_iter>`
//!
//! Emits a JSON record on the last line of stdout with the canonical
//! `execution_time_ms` / `total_time_ms` keys consumed by the campaign
//! orchestrator's `cost_backends.run_standalone_remote()`.

use pagerank_core::{build_csr, load_edges, run_pagerank, DEFAULT_DAMPING, DEFAULT_TOLERANCE};
use std::env;
use std::time::Instant;

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 4 {
        eprintln!("Usage: {} <graph_file> <num_nodes> <max_iter>", args[0]);
        std::process::exit(1);
    }
    let graph_file = &args[1];
    let num_nodes: u32 = args[2].parse().expect("Invalid num_nodes");
    let max_iter: u32 = args[3].parse().expect("Invalid max_iter");

    let start_load = Instant::now();
    let edges = load_edges(graph_file);
    let load_ms = start_load.elapsed().as_millis() as u64;

    let start_build = Instant::now();
    let csr = build_csr(num_nodes, &edges);
    let build_ms = start_build.elapsed().as_millis() as u64;

    let start_pr = Instant::now();
    let (rank, iters) = run_pagerank(&csr, max_iter, DEFAULT_DAMPING, DEFAULT_TOLERANCE);
    let exec_ms = start_pr.elapsed().as_millis() as u64;

    let max_rank = rank.iter().cloned().fold(0.0f32, f32::max);
    let sum_rank: f64 = rank.iter().map(|&r| r as f64).sum();

    let record = serde_json::json!({
        "load_time_ms": load_ms,
        "build_time_ms": build_ms,
        "execution_time_ms": exec_ms,
        "total_time_ms": load_ms + build_ms + exec_ms,
        "num_nodes": num_nodes,
        "num_edges": csr.num_edges(),
        "iterations": iters,
        "damping": DEFAULT_DAMPING,
        "max_rank": max_rank,
        "sum_rank": sum_rank,
        "rank": rank,
    });
    println!("{}", record);
}
