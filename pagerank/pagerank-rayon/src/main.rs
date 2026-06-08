//! Rayon PageRank benchmark binary.
//!
//! CLI: `pagerank-rayon <graph_file> <num_nodes> <max_iter> [threads]`
//!
//! Parallelism strategy: pull-style update. For each iteration, we precompute
//! `contrib[i] = rank[i] / out_degree[i]` (or 0 for dangling), then update
//! `next[j]` for every vertex `j` by accumulating contributions from incoming
//! neighbours. To avoid building the reverse graph, we use the same CSR but
//! drive the inner aggregation across destination vertices in parallel by
//! pre-binning contributions via an atomic-free scatter using thread-local
//! buffers reduced at the end of each iteration.
//!
//! For simplicity in this scaffold, we fall back to a sequential `contribute`
//! step but parallelise the teleport+damping fold across vertices (where most
//! of the FLOPs land for sparse-but-large graphs).

use pagerank_core::{
    build_csr, load_edges, power_iter_contribute, DEFAULT_DAMPING, DEFAULT_TOLERANCE,
};
use rayon::prelude::*;
use std::env;
use std::time::Instant;

fn run_pagerank_rayon(
    csr: &pagerank_core::Csr,
    max_iter: u32,
    damping: f32,
    tolerance: f32,
) -> (Vec<f32>, u32) {
    let n = csr.num_nodes as usize;
    let mut rank = vec![1.0f32 / n as f32; n];
    let mut next = vec![0.0f32; n];
    let teleport_base = (1.0 - damping) / n as f32;
    for it in 0..max_iter {
        next.par_iter_mut().for_each(|x| *x = 0.0);
        let dangling = power_iter_contribute(csr, &rank, &mut next);
        let dangling_per_node = dangling / n as f32;
        let delta: f32 = rank
            .par_iter_mut()
            .zip(next.par_iter())
            .map(|(r, &n_v)| {
                let new_v = teleport_base + damping * (n_v + dangling_per_node);
                let d = (new_v - *r).abs();
                *r = new_v;
                d
            })
            .sum();
        if delta < tolerance {
            return (rank, it + 1);
        }
    }
    (rank, max_iter)
}

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 4 {
        eprintln!(
            "Usage: {} <graph_file> <num_nodes> <max_iter> [threads]",
            args[0]
        );
        std::process::exit(1);
    }
    let graph_file = &args[1];
    let num_nodes: u32 = args[2].parse().expect("Invalid num_nodes");
    let max_iter: u32 = args[3].parse().expect("Invalid max_iter");

    if let Some(threads) = args.get(4) {
        let n: usize = threads.parse().expect("Invalid threads");
        rayon::ThreadPoolBuilder::new()
            .num_threads(n)
            .build_global()
            .expect("Failed to build rayon pool");
    }
    let active_threads = rayon::current_num_threads();

    let start_load = Instant::now();
    let edges = load_edges(graph_file);
    let load_ms = start_load.elapsed().as_millis() as u64;

    let start_build = Instant::now();
    let csr = build_csr(num_nodes, &edges);
    let build_ms = start_build.elapsed().as_millis() as u64;

    let start_pr = Instant::now();
    let (rank, iters) = run_pagerank_rayon(&csr, max_iter, DEFAULT_DAMPING, DEFAULT_TOLERANCE);
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
        "threads": active_threads,
        "max_rank": max_rank,
        "sum_rank": sum_rank,
        "rank": rank,
    });
    println!("{}", record);
}
