//! # Label Propagation Runner CLI
//!
//! Command-line tool for running the label propagation algorithm on a graph
//! loaded from a JSON file.
//!
//! ## Usage
//!
//! ```bash
//! run_label_propagation <input_graph> [max_iterations] [convergence_threshold] [output_file]
//! ```
//!
//! ## Arguments
//!
//! - `input_graph`: Path to JSON file containing graph data
//! - `max_iterations`: Maximum iterations (default: 100)
//! - `convergence_threshold`: Convergence threshold (default: 0.01)
//! - `output_file`: Output JSON file path (default: results.json)

use label_propagation::graph_generator::load_graph;
use label_propagation::{Graph, LabelPropagation};
use std::env;
use std::fs;
use serde_json;
use std::time::Instant;

/// Prints usage information for the command-line interface
fn print_usage() {
    println!("Usage: run_label_propagation <input_graph> [max_iterations] [convergence_threshold] [output_file]");
    println!("\nArguments:");
    println!("  input_graph           - Input JSON file with graph data");
    println!("  max_iterations        - Maximum iterations (default: 100)");
    println!("  convergence_threshold - Convergence threshold (default: 0.01)");
    println!("  output_file           - Output JSON file (default: results.json)");
    println!("\nExample:");
    println!("  run_label_propagation graph.json 100 0.01 results.json");
}

fn main() {
    let start_time = Instant::now();
    let args: Vec<String> = env::args().collect();

    // Validate minimum required arguments
    if args.len() < 2 {
        print_usage();
        std::process::exit(1);
    }

    // Parse command-line arguments with defaults
    let input_file = &args[1];
    let max_iterations: usize = args.get(2)
        .and_then(|s| s.parse().ok())
        .unwrap_or(100);
    let convergence_threshold: f64 = args.get(3)
        .and_then(|s| s.parse().ok())
        .unwrap_or(0.01);
    let output_file = args.get(4)
        .map(|s| s.as_str())
        .unwrap_or("results.json");

    println!("Loading graph from {}...", input_file);
    
    // Load graph from JSON file
    let graph_data = match load_graph(input_file) {
        Ok(data) => data,
        Err(e) => {
            eprintln!("Error loading graph: {}", e);
            std::process::exit(1);
        }
    };

    // Display graph statistics
    println!("Graph loaded:");
    println!("  Nodes: {}", graph_data.num_nodes);
    println!("  Edges: {}", graph_data.edges.len());
    println!("  Initially labeled: {}", graph_data.labeled_nodes.len());

    // Convert to internal graph representation
    let graph = Graph::from_edge_list(graph_data.edges, graph_data.labeled_nodes);

    println!("\nRunning Label Propagation...");
    println!("  Max iterations: {}", max_iterations);
    println!("  Convergence threshold: {}", convergence_threshold);

    // Run the label propagation algorithm (timed)
    let propagation_start = Instant::now();
    let lp = LabelPropagation::new(max_iterations, convergence_threshold);
    let result = lp.propagate(&graph);
    let propagation_time = propagation_start.elapsed();

    // Display results
    println!("\nResults:");
    println!("  Converged: {}", result.converged);
    println!("  Iterations: {}", result.iterations);
    println!("  Total labeled nodes: {}", result.labels.len());
    println!("  Propagation time: {:.3}s ({:.2}ms)", propagation_time.as_secs_f64(), propagation_time.as_micros() as f64 / 1000.0);

    // Count and display label distribution
    let mut label_counts = std::collections::HashMap::new();
    for label in result.labels.values() {
        *label_counts.entry(*label).or_insert(0) += 1;
    }

    println!("\nLabel distribution:");
    let mut sorted_labels: Vec<_> = label_counts.iter().collect();
    sorted_labels.sort_by_key(|(label, _)| *label);
    for (label, count) in sorted_labels {
        println!("  Label {}: {} nodes", label, count);
    }

    // Save results to JSON file
    match serde_json::to_string_pretty(&result) {
        Ok(json) => {
            if let Err(e) = fs::write(output_file, json) {
                eprintln!("Error writing results: {}", e);
                std::process::exit(1);
            } else {
                println!("\nResults saved to {}", output_file);
            }
        }
        Err(e) => {
            eprintln!("Error serializing results: {}", e);
            std::process::exit(1);
        }
    }
    
    // Display total execution time (includes I/O)
    let elapsed = start_time.elapsed();
    println!("\nTotal execution time: {:.3}s ({:.2}ms)", elapsed.as_secs_f64(), elapsed.as_micros() as f64 / 1000.0);
}
