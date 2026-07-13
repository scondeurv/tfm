//! Rayon PageRank benchmark binary.
//!
//! CLI: `pagerank-rayon <graph_file> <num_nodes> <max_iter> [threads]`
//!
//! Parallelism strategy: pull-style update over a transposed CSR. Each
//! iteration (1) precomputes `contrib[u] = rank[u] / out_degree[u]` (0 for
//! dangling) in parallel, then (2) computes `next[v]` for every destination
//! `v` in parallel by summing `contrib[u]` over its in-neighbours `u`. Because
//! every destination owns its own `next[v]` slot, the scatter is contention-
//! free: no atomics, no thread-local reduction. The transpose is built once,
//! amortised over all iterations. This is the standard high-performance
//! shared-memory PageRank formulation; it replaces the earlier scaffold whose
//! `contribute` step ran serially and therefore never scaled.

use pagerank_core::{
    build_csr, build_csr_transpose, load_edges, DEFAULT_DAMPING, DEFAULT_TOLERANCE,
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
    let transpose = build_csr_transpose(csr);
    let mut rank = vec![1.0f32 / n as f32; n];
    let mut contrib = vec![0.0f32; n];
    let mut next = vec![0.0f32; n];
    let teleport_base = (1.0 - damping) / n as f32;
    for it in 0..max_iter {
        // (1) parallel precompute of per-source contribution + dangling mass.
        let dangling: f32 = contrib
            .par_iter_mut()
            .enumerate()
            .map(|(u, c)| {
                let deg = csr.out_degree[u];
                if deg == 0 {
                    *c = 0.0;
                    rank[u]
                } else {
                    *c = rank[u] / deg as f32;
                    0.0
                }
            })
            .sum();
        let dangling_per_node = dangling / n as f32;
        // (2) parallel pull: each destination sums its in-neighbours' contrib.
        next.par_iter_mut().enumerate().for_each(|(v, nv)| {
            let start = transpose.in_offsets[v] as usize;
            let end = transpose.in_offsets[v + 1] as usize;
            let mut acc = 0.0f32;
            for k in start..end {
                acc += contrib[transpose.in_neighbors[k] as usize];
            }
            *nv = teleport_base + damping * (acc + dangling_per_node);
        });
        // (3) parallel commit + L1 delta.
        let delta: f32 = rank
            .par_iter_mut()
            .zip(next.par_iter())
            .map(|(r, &n_v)| {
                let d = (n_v - *r).abs();
                *r = n_v;
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
