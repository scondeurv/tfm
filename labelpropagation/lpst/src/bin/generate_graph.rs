//! # Graph Generator CLI
//!
//! Command-line tool for generating various graph topologies for testing
//! the label propagation algorithm.
//!
//! ## Usage
//!
//! ```bash
//! generate_graph <type> <num_nodes> <num_labels> <label_percentage> <output_file>
//! ```
//!
//! ## Graph Types
//!
//! - `random`: Erdős-Rényi random graph
//! - `grid`: 2D grid/lattice
//! - `ring`: Circular graph
//! - `smallworld`: Watts-Strogatz small-world network
//! - `community`: Graph with community structure

use label_propagation::graph_generator::{GraphGenerator, GraphType, save_graph};
use std::env;
use std::time::Instant;

/// Prints usage information for the command-line interface
fn print_usage() {
    println!("Usage: generate_graph <type> <num_nodes> <num_labels> <label_percentage> <output_file>");
    println!("\nGraph types:");
    println!("  random      - Random graph with probability 0.3");
    println!("  grid        - 2D grid graph");
    println!("  ring        - Ring graph (circle)");
    println!("  smallworld  - Watts-Strogatz small-world graph");
    println!("  community   - Community structure graph");
    println!("\nArguments:");
    println!("  num_nodes         - Number of nodes in the graph");
    println!("  num_labels        - Number of different labels");
    println!("  label_percentage  - Percentage of nodes to label initially (0.0-1.0)");
    println!("  output_file       - Output JSON file path");
    println!("\nExample:");
    println!("  generate_graph community 100 3 0.1 graph.json");
}

fn main() {
    let start_time = Instant::now();
    let args: Vec<String> = env::args().collect();

    // Validate command-line arguments
    if args.len() != 6 {
        print_usage();
        std::process::exit(1);
    }

    // Parse command-line arguments
    let graph_type_str = &args[1];
    let num_nodes: usize = args[2].parse().expect("Invalid number of nodes");
    let num_labels: usize = args[3].parse().expect("Invalid number of labels");
    let label_percentage: f64 = args[4].parse().expect("Invalid label percentage");
    let output_file = &args[5];

    // Validate label percentage range
    if label_percentage < 0.0 || label_percentage > 1.0 {
        eprintln!("Error: label_percentage must be between 0.0 and 1.0");
        std::process::exit(1);
    }

    // Map string to GraphType enum
    let graph_type = match graph_type_str.as_str() {
        "random" => GraphType::Random,
        "grid" => GraphType::Grid,
        "ring" => GraphType::Ring,
        "smallworld" => GraphType::SmallWorld,
        "community" => GraphType::Community,
        _ => {
            eprintln!("Error: Unknown graph type '{}'", graph_type_str);
            print_usage();
            std::process::exit(1);
        }
    };

    println!("Generating {} graph with {} nodes...", graph_type_str, num_nodes);
    
    // Generate the graph
    let generator = GraphGenerator::new(num_nodes, graph_type);
    let graph_data = generator.generate(num_labels, label_percentage);

    // Display statistics
    println!("Graph statistics:");
    println!("  Nodes: {}", graph_data.num_nodes);
    println!("  Edges: {}", graph_data.edges.len());
    println!("  Labeled nodes: {}", graph_data.labeled_nodes.len());
    println!("  Labels used: {}", num_labels);

    // Save graph to file
    match save_graph(&graph_data, output_file) {
        Ok(_) => println!("\nGraph saved to {}", output_file),
        Err(e) => {
            eprintln!("Error saving graph: {}", e);
            std::process::exit(1);
        }
    }
    
    // Display execution time
    let elapsed = start_time.elapsed();
    println!("\nTotal execution time: {:.3}s ({:.2}ms)", elapsed.as_secs_f64(), elapsed.as_micros() as f64 / 1000.0);
}
