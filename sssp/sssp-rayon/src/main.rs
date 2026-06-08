//! Rayon SSSP benchmark binary. Reads a weighted-edge TSV
//! `src\tdst\tweight[\tlabel]` and runs synchronous Bellman-Ford in parallel.

use sssp_rayon::run_bellman_ford_rayon;
use sssp_standalone::build_csr_from_edges;
use std::env;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::time::Instant;

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 5 {
        eprintln!("Usage: {} <graph_file> <num_nodes> <source> <max_iter> [threads]", args[0]);
        std::process::exit(1);
    }

    let graph_file = &args[1];
    let num_nodes: u32 = args[2].parse().expect("Invalid num_nodes");
    let source: u32 = args[3].parse().expect("Invalid source");
    let max_iter: u32 = args[4].parse().expect("Invalid max_iter");

    if let Some(threads) = args.get(5) {
        let n: usize = threads.parse().expect("Invalid threads");
        rayon::ThreadPoolBuilder::new()
            .num_threads(n)
            .build_global()
            .expect("Failed to build rayon pool");
    }
    let active_threads = rayon::current_num_threads();

    let start_load = Instant::now();
    let edges = load_edges(graph_file);
    let load_duration = start_load.elapsed();

    let start_build = Instant::now();
    let csr = build_csr_from_edges(num_nodes, &edges);
    let build_duration = start_build.elapsed();

    let start_lp = Instant::now();
    let result = run_bellman_ford_rayon(&csr, source, max_iter);
    let lp_duration = start_lp.elapsed();

    let out = serde_json::json!({
        "load_time_ms": load_duration.as_millis(),
        "build_time_ms": build_duration.as_millis(),
        "execution_time_ms": lp_duration.as_millis(),
        "total_time_ms": (load_duration + build_duration + lp_duration).as_millis(),
        "num_nodes": num_nodes,
        "num_edges": csr.num_edges(),
        "threads": active_threads,
        "reachable_nodes": result.reachable_nodes,
        "max_distance": result.max_distance,
        "distances": result.distances,
    });
    println!("{}", out);
}

fn load_edges(path: &str) -> Vec<(u32, u32, f32)> {
    let file = File::open(path).expect("Could not open graph file");
    let reader = BufReader::new(file);
    let mut edges: Vec<(u32, u32, f32)> = Vec::new();
    for line in reader.lines() {
        let line = line.expect("Could not read line");
        if line.trim().is_empty() {
            continue;
        }
        let parts: Vec<&str> = line.split('\t').collect();
        if parts.len() < 3 {
            continue;
        }
        let src: u32 = parts[0].parse().expect("Invalid src");
        let dst: u32 = parts[1].parse().expect("Invalid dst");
        let w: f32 = parts[2].parse().expect("Invalid weight");
        edges.push((src, dst, w));
    }
    edges
}
