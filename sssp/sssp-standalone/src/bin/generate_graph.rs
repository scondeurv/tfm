//! Generate weighted random graphs for SSSP benchmarking.
//!
//! Usage:
//!   generate-sssp-graph <num_nodes> <density> <output_file> [max_weight] [seed]

use std::env;
use std::fs::File;
use std::io::{BufWriter, Write};

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 4 {
        eprintln!(
            "Usage: {} <num_nodes> <density> <output_file> [max_weight] [seed]",
            args[0]
        );
        std::process::exit(1);
    }

    let num_nodes: u64 = args[1].parse().expect("Invalid num_nodes");
    let density: u64 = args[2].parse().expect("Invalid density");
    let output_file = &args[3];
    let max_weight: f32 = args
        .get(4)
        .and_then(|s| s.parse().ok())
        .unwrap_or(10.0);
    let seed: u64 = args.get(5).and_then(|s| s.parse().ok()).unwrap_or(42);

    eprintln!(
        "Generating SSSP graph: {} nodes, density={}, max_weight={:.1}, seed={}",
        num_nodes, density, max_weight, seed
    );

    let file = File::create(output_file).expect("Could not create output file");
    let mut writer = BufWriter::new(file);
    let mut rng = seed;
    let mut edge_count: u64 = 0;

    for src in 0..num_nodes {
        for _ in 0..density {
            // xorshift64
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;

            let mut dst = (rng % (num_nodes - 1)) as u64;
            if dst >= src {
                dst += 1;
            }

            // generate weight in [0.1, max_weight]
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            let weight = 0.1 + (rng as f64 / u64::MAX as f64) * (max_weight as f64 - 0.1);

            writeln!(writer, "{}\t{}\t{:.4}", src, dst, weight).unwrap();
            edge_count += 1;
        }
    }

    eprintln!("Wrote {} edges to {}", edge_count, output_file);
}
