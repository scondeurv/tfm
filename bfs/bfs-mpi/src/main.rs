//! MPI BFS benchmark binary. Run with:
//!
//!   mpirun -np <p> -H compute6,compute7 ./bfs-mpi <graph_file> <num_nodes> <source> <max_levels>

use bfs_mpi::run_bfs_mpi;
use bfs_standalone::build_csr_from_edges;
use mpi::traits::*;
use std::env;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::time::Instant;

fn main() {
    let universe = mpi::initialize().expect("MPI init failed");
    let world = universe.world();
    let rank = world.rank();

    let args: Vec<String> = env::args().collect();
    if args.len() < 5 {
        if rank == 0 {
            eprintln!("Usage: {} <graph_file> <num_nodes> <source> <max_levels>", args[0]);
        }
        std::process::exit(1);
    }

    let graph_file = &args[1];
    let num_nodes: u32 = args[2].parse().expect("Invalid num_nodes");
    let source: u32 = args[3].parse().expect("Invalid source");
    let max_levels: u32 = args[4].parse().expect("Invalid max_levels");

    let start_load = Instant::now();
    let edges = load_edges(graph_file);
    let load_duration = start_load.elapsed();

    let start_build = Instant::now();
    let csr = build_csr_from_edges(num_nodes, &edges);
    let build_duration = start_build.elapsed();

    let start_run = Instant::now();
    let result = run_bfs_mpi(&world, &csr, source, max_levels);
    let run_duration = start_run.elapsed();

    if rank == 0 {
        let out = serde_json::json!({
            "load_time_ms": load_duration.as_millis(),
            "build_time_ms": build_duration.as_millis(),
            "execution_time_ms": run_duration.as_millis(),
            "total_time_ms": (load_duration + build_duration + run_duration).as_millis(),
            "num_nodes": num_nodes,
            "num_edges": csr.num_edges(),
            "ranks": world.size(),
            "visited_nodes": result.visited_nodes,
            "max_level": result.max_level,
            "levels": result.levels,
        });
        println!("{}", out);
    }
}

fn load_edges(path: &str) -> Vec<(u32, u32)> {
    let file = File::open(path).expect("Could not open graph file");
    let reader = BufReader::new(file);
    let mut edges: Vec<(u32, u32)> = Vec::new();
    for line in reader.lines() {
        let line = line.expect("Could not read line");
        if line.trim().is_empty() {
            continue;
        }
        let parts: Vec<&str> = line.split('\t').collect();
        if parts.len() < 2 {
            continue;
        }
        let src: u32 = parts[0].parse().expect("Invalid src");
        let dst: u32 = parts[1].parse().expect("Invalid dst");
        edges.push((src, dst));
    }
    edges
}
