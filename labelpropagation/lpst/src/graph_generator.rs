//! # Graph Generator Module
//!
//! This module provides utilities for generating various types of graph topologies
//! for testing and benchmarking the label propagation algorithm.
//!
//! ## Supported Graph Types
//!
//! - **Random**: Erdős-Rényi random graphs
//! - **Grid**: 2D grid/lattice graphs
//! - **Ring**: Circular graphs
//! - **Small-World**: Watts-Strogatz small-world graphs
//! - **Community**: Graphs with community structure (high intra-community edges, low inter-community edges)

use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::fs;
use rand::Rng;

/// Serializable graph data structure for file I/O
#[derive(Debug, Serialize, Deserialize)]
pub struct GraphData {
    /// List of edges as (from, to) tuples
    pub edges: Vec<(usize, usize)>,
    /// Initially labeled nodes: maps node ID to label
    pub labeled_nodes: HashMap<usize, usize>,
    /// Total number of nodes in the graph
    pub num_nodes: usize,
}

/// Supported graph topology types
pub enum GraphType {
    /// Random graph (Erdős-Rényi model)
    Random,
    /// 2D grid/lattice
    Grid,
    /// Ring/circle graph
    Ring,
    /// Small-world network (Watts-Strogatz model)
    SmallWorld,
    /// Graph with community structure
    Community,
}

/// Generator for creating graphs with different topologies
pub struct GraphGenerator {
    /// Number of nodes in the graph
    num_nodes: usize,
    /// Type of graph topology to generate
    graph_type: GraphType,
}

impl GraphGenerator {
    /// Creates a new graph generator
    ///
    /// # Arguments
    ///
    /// * `num_nodes` - Number of nodes in the graph
    /// * `graph_type` - Type of topology to generate
    pub fn new(num_nodes: usize, graph_type: GraphType) -> Self {
        GraphGenerator {
            num_nodes,
            graph_type,
        }
    }

    /// Generates a complete graph with edges and labeled nodes
    ///
    /// # Arguments
    ///
    /// * `num_labels` - Number of distinct labels to use
    /// * `label_percentage` - Fraction of nodes to label initially (0.0 to 1.0)
    ///
    /// # Returns
    ///
    /// A `GraphData` structure containing edges and labeled nodes
    pub fn generate(&self, num_labels: usize, label_percentage: f64) -> GraphData {
        let edges = match self.graph_type {
            GraphType::Random => self.generate_random_graph(0.3),
            GraphType::Grid => self.generate_grid_graph(),
            GraphType::Ring => self.generate_ring_graph(),
            GraphType::SmallWorld => self.generate_small_world_graph(4, 0.3),
            GraphType::Community => self.generate_community_graph(num_labels, 0.7, 0.05),
        };

        let labeled_nodes = self.generate_labels(num_labels, label_percentage);

        GraphData {
            edges,
            labeled_nodes,
            num_nodes: self.num_nodes,
        }
    }

    /// Generates a random graph using the Erdős-Rényi model
    ///
    /// # Arguments
    ///
    /// * `probability` - Probability of edge creation between any two nodes (0.0 to 1.0)
    ///
    /// # Returns
    ///
    /// Vector of edges
    fn generate_random_graph(&self, probability: f64) -> Vec<(usize, usize)> {
        let mut rng = rand::thread_rng();
        let mut edges = Vec::new();

        // Try to create an edge between every pair of nodes
        for i in 0..self.num_nodes {
            for j in (i + 1)..self.num_nodes {
                if rng.gen::<f64>() < probability {
                    edges.push((i, j));
                }
            }
        }

        edges
    }

    /// Generates a 2D grid/lattice graph
    ///
    /// # Returns
    ///
    /// Vector of edges forming a grid structure
    fn generate_grid_graph(&self) -> Vec<(usize, usize)> {
        let side = (self.num_nodes as f64).sqrt().ceil() as usize;
        let mut edges = Vec::new();

        for i in 0..self.num_nodes {
            let row = i / side;
            let col = i % side;

            // Connect to right neighbor
            if col < side - 1 && i + 1 < self.num_nodes {
                edges.push((i, i + 1));
            }

            // Connect to bottom neighbor
            if row < side - 1 && i + side < self.num_nodes {
                edges.push((i, i + side));
            }
        }

        edges
    }

    /// Generates a ring (circular) graph
    ///
    /// # Returns
    ///
    /// Vector of edges forming a ring
    fn generate_ring_graph(&self) -> Vec<(usize, usize)> {
        let mut edges = Vec::new();

        // Connect each node to the next one, wrapping around at the end
        for i in 0..self.num_nodes {
            edges.push((i, (i + 1) % self.num_nodes));
        }

        edges
    }

    /// Generates a small-world graph using the Watts-Strogatz model
    ///
    /// # Arguments
    ///
    /// * `k` - Each node is connected to k nearest neighbors in ring topology
    /// * `rewire_prob` - Probability of rewiring each edge (0.0 to 1.0)
    ///
    /// # Returns
    ///
    /// Vector of edges forming a small-world network
    fn generate_small_world_graph(&self, k: usize, rewire_prob: f64) -> Vec<(usize, usize)> {
        let mut rng = rand::thread_rng();
        let mut edges = HashSet::new();

        // Step 1: Create a ring lattice where each node connects to k/2 nearest neighbors
        for i in 0..self.num_nodes {
            for j in 1..=k / 2 {
                let neighbor = (i + j) % self.num_nodes;
                edges.insert(if i < neighbor { (i, neighbor) } else { (neighbor, i) });
            }
        }

        // Step 2: Rewire edges with given probability
        let edges_vec: Vec<_> = edges.iter().cloned().collect();
        let mut final_edges = HashSet::new();

        for (u, v) in edges_vec {
            if rng.gen::<f64>() < rewire_prob {
                // Rewire: choose a new random target node
                let mut new_v = rng.gen_range(0..self.num_nodes);
                let mut attempts = 0;
                // Avoid self-loops and duplicate edges
                while (new_v == u || final_edges.contains(&(u.min(new_v), u.max(new_v)))) 
                    && attempts < 100 {
                    new_v = rng.gen_range(0..self.num_nodes);
                    attempts += 1;
                }
                if attempts < 100 {
                    final_edges.insert((u.min(new_v), u.max(new_v)));
                } else {
                    // Keep original edge if rewiring fails
                    final_edges.insert((u, v));
                }
            } else {
                // Keep original edge
                final_edges.insert((u, v));
            }
        }

        final_edges.into_iter().collect()
    }

    /// Generates a graph with community structure
    ///
    /// # Arguments
    ///
    /// * `num_communities` - Number of communities
    /// * `intra_prob` - Probability of edges within communities
    /// * `inter_prob` - Probability of edges between communities
    ///
    /// # Returns
    ///
    /// Vector of edges forming a community-structured graph
    fn generate_community_graph(
        &self,
        num_communities: usize,
        intra_prob: f64,
        inter_prob: f64,
    ) -> Vec<(usize, usize)> {
        let mut rng = rand::thread_rng();
        let mut edges = Vec::new();
        let nodes_per_community = self.num_nodes / num_communities;

        for i in 0..self.num_nodes {
            for j in (i + 1)..self.num_nodes {
                // Determine which community each node belongs to
                let comm_i = i / nodes_per_community.max(1);
                let comm_j = j / nodes_per_community.max(1);

                // Use different edge probabilities for intra vs inter-community edges
                let prob = if comm_i == comm_j {
                    intra_prob  // High probability within same community
                } else {
                    inter_prob  // Low probability between different communities
                };

                if rng.gen::<f64>() < prob {
                    edges.push((i, j));
                }
            }
        }

        edges
    }

    /// Generates initial labels for a subset of nodes
    ///
    /// Labels are distributed randomly among nodes, with each label getting
    /// approximately equal number of nodes.
    ///
    /// # Arguments
    ///
    /// * `num_labels` - Number of distinct labels to use
    /// * `percentage` - Fraction of nodes to label (0.0 to 1.0)
    ///
    /// # Returns
    ///
    /// HashMap mapping node IDs to their initial labels
    fn generate_labels(&self, num_labels: usize, percentage: f64) -> HashMap<usize, usize> {
        let mut rng = rand::thread_rng();
        let mut labeled = HashMap::new();
        let num_to_label = (self.num_nodes as f64 * percentage).ceil() as usize;
        let nodes_per_label = num_to_label / num_labels;

        let mut available_nodes: Vec<usize> = (0..self.num_nodes).collect();
        
        // Distribute labels evenly across labeled nodes
        for label in 0..num_labels {
            let count = if label == num_labels - 1 {
                // Last label gets any remaining nodes
                num_to_label - (nodes_per_label * (num_labels - 1))
            } else {
                nodes_per_label
            };

            // Randomly select nodes for this label
            for _ in 0..count.min(available_nodes.len()) {
                let idx = rng.gen_range(0..available_nodes.len());
                let node = available_nodes.remove(idx);
                labeled.insert(node, label);
            }
        }

        labeled
    }
}

/// Saves graph data to a JSON file
///
/// # Arguments
///
/// * `graph_data` - Graph data to save
/// * `filename` - Output file path
///
/// # Returns
///
/// Result indicating success or I/O error
pub fn save_graph(graph_data: &GraphData, filename: &str) -> std::io::Result<()> {
    let json = serde_json::to_string_pretty(graph_data)?;
    fs::write(filename, json)?;
    Ok(())
}

/// Loads graph data from a JSON file
///
/// # Arguments
///
/// * `filename` - Input file path
///
/// # Returns
///
/// Result containing graph data or I/O error
pub fn load_graph(filename: &str) -> std::io::Result<GraphData> {
    let json = fs::read_to_string(filename)?;
    let graph_data = serde_json::from_str(&json)?;
    Ok(graph_data)
}
