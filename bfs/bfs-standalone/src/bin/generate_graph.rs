//! Generate deterministic random BFS benchmark graphs.
//!
//! Each node gets `density` random outgoing edges (no self-loops, no duplicates).
//! Uses a simple LCG PRNG so the same seed always produces the same graph —
//! no external dependency on the `rand` crate needed.
//!
//! Usage:
//!   generate-bfs-graph <num_nodes> <output_file> [density] [seed]

use std::collections::HashSet;
use std::env;
use std::fs::File;
use std::io::{BufWriter, Write};

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 3 {
        eprintln!(
            "Usage: {} <num_nodes> <output_file> [density] [seed]",
            args[0]
        );
        std::process::exit(1);
    }

    let num_nodes: u32 = args[1].parse().expect("Invalid num_nodes");
    let output_file = &args[2];
    let density: u32 = args.get(3).and_then(|s| s.parse().ok()).unwrap_or(10);
    let seed: u64 = args.get(4).and_then(|s| s.parse().ok()).unwrap_or(42);

    let file = File::create(output_file).expect("Could not create output file");
    let mut writer = BufWriter::new(file);

    // LCG PRNG – deterministic, fast, no deps
    let mut state = seed;
    let mult: u64 = 6364136223846793005;
    let inc: u64 = 1442695040888963407;
    let mut next_rand = move || -> u32 {
        state = state.wrapping_mul(mult).wrapping_add(inc);
        (state >> 33) as u32
    };

    let mut total_edges: u64 = 0;

    for i in 0..num_nodes {
        let max_targets = density.min(num_nodes.saturating_sub(1));
        let mut seen: HashSet<u32> = HashSet::new();
        seen.insert(i); // no self-loops
        let mut added = 0u32;
        let mut attempts = 0u32;

        while added < max_targets && attempts < max_targets * 20 {
            let t = next_rand() % num_nodes;
            if seen.insert(t) {
                writeln!(writer, "{}\t{}", i, t).expect("Write failed");
                total_edges += 1;
                added += 1;
            }
            attempts += 1;
        }
    }

    eprintln!(
        "Generated {} nodes, {} edges (density={})",
        num_nodes, total_edges, density
    );
}
