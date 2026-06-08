//! MPI PageRank benchmark binary.
//!
//! CLI: `mpirun -np <p> -H compute6,compute7 ./pagerank-mpi <graph_file> <num_nodes> <max_iter>`
//!
//! Rank 0 prints the canonical JSON record on the last line of stdout. Other
//! ranks stay silent so the orchestrator's `_parse_benchmark_stdout()` keeps
//! seeing exactly one JSON line per cell.

use mpi::traits::*;
use pagerank_core::{build_csr, load_edges, DEFAULT_DAMPING, DEFAULT_TOLERANCE};
use pagerank_mpi::run_pagerank_mpi;
use std::env;
use std::time::Instant;

fn main() {
    let universe = mpi::initialize().expect("MPI init failed");
    let world = universe.world();
    let rank = world.rank();

    let args: Vec<String> = env::args().collect();
    if args.len() < 4 {
        if rank == 0 {
            eprintln!("Usage: {} <graph_file> <num_nodes> <max_iter>", args[0]);
        }
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

    let start_run = Instant::now();
    let result = run_pagerank_mpi(&world, &csr, max_iter, DEFAULT_DAMPING, DEFAULT_TOLERANCE);
    let exec_ms = start_run.elapsed().as_millis() as u64;

    if rank == 0 {
        let record = serde_json::json!({
            "load_time_ms": load_ms,
            "build_time_ms": build_ms,
            "execution_time_ms": exec_ms,
            "total_time_ms": load_ms + build_ms + exec_ms,
            "num_nodes": num_nodes,
            "num_edges": csr.num_edges(),
            "ranks": world.size(),
            "iterations": result.iterations,
            "damping": DEFAULT_DAMPING,
            "max_rank": result.max_rank,
            "sum_rank": result.sum_rank,
            "rank": result.rank,
        });
        println!("{}", record);
    }
}
