//! # Label Propagation Algorithm Library
//!
//! This library implements a Semi-Supervised Label Propagation algorithm for graph-based
//! semi-supervised learning. The algorithm propagates labels through a graph structure
//! based on neighborhood connectivity.
//!
//! ## Features
//!
//! - **Deterministic**: Uses consistent tie-breaking for reproducible results
//! - **Memory Efficient**: Uses change tracking instead of full cloning
//! - **Fast**: Synchronous updates with early convergence detection
//! - **Clamping**: Initial labels are preserved (semi-supervised)

pub mod graph_generator;

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::cmp::Ordering;

pub const UNKNOWN: u32 = u32::MAX;

/// Compressed Sparse Row representation of an undirected graph.
///
/// `row_offsets[i]..row_offsets[i+1]` indexes into `dst` to give the
/// adjacency list of node `i`. Length of `row_offsets` is `num_nodes + 1`.
#[derive(Debug, Clone)]
pub struct CsrGraph {
    pub num_nodes: u32,
    pub row_offsets: Vec<u32>,
    pub dst: Vec<u32>,
}

impl CsrGraph {
    pub fn neighbors(&self, node: u32) -> &[u32] {
        let i = node as usize;
        let start = self.row_offsets[i] as usize;
        let end = self.row_offsets[i + 1] as usize;
        &self.dst[start..end]
    }

    pub fn num_edges(&self) -> usize {
        self.dst.len()
    }
}

/// Build a CSR graph from a directed edge list. Each `(u, v)` becomes one entry
/// in `u`'s adjacency. Callers wanting undirected graphs must include both
/// `(u, v)` and `(v, u)` in `edges`.
pub fn build_csr_from_edges(num_nodes: u32, edges: &[(u32, u32)]) -> CsrGraph {
    let n = num_nodes as usize;
    let mut row_offsets = vec![0u32; n + 1];
    for &(src, _) in edges {
        if (src as usize) < n {
            row_offsets[src as usize + 1] += 1;
        }
    }
    for i in 1..=n {
        row_offsets[i] += row_offsets[i - 1];
    }
    let total = row_offsets[n] as usize;
    let mut dst = vec![0u32; total];
    let mut cursor = row_offsets.clone();
    for &(src, dst_node) in edges {
        let i = src as usize;
        if i < n {
            let pos = cursor[i] as usize;
            dst[pos] = dst_node;
            cursor[i] += 1;
        }
    }
    CsrGraph { num_nodes, row_offsets, dst }
}

/// Build CSR from a HashMap adjacency. Used by the legacy [`run_lp`] wrapper
/// to keep backwards compatibility.
pub fn build_csr_from_adj(adj: &HashMap<u32, Vec<u32>>, num_nodes: u32) -> CsrGraph {
    let n = num_nodes as usize;
    let mut row_offsets = vec![0u32; n + 1];
    for (&src, neigh) in adj.iter() {
        if (src as usize) < n {
            row_offsets[src as usize + 1] = neigh.len() as u32;
        }
    }
    for i in 1..=n {
        row_offsets[i] += row_offsets[i - 1];
    }
    let total = row_offsets[n] as usize;
    let mut dst = vec![0u32; total];
    for i in 0..num_nodes {
        if let Some(neigh) = adj.get(&i) {
            let start = row_offsets[i as usize] as usize;
            for (k, &v) in neigh.iter().enumerate() {
                dst[start + k] = v;
            }
        }
    }
    CsrGraph { num_nodes, row_offsets, dst }
}

pub fn majority_label(counts: &HashMap<u32, usize>, current: u32) -> u32 {
    if counts.is_empty() {
        return current;
    }
    let mut best = current;
    let mut best_count = 0usize;
    for (label, count) in counts {
        if *label == UNKNOWN {
            continue;
        }
        match count.cmp(&best_count) {
            Ordering::Greater => {
                best = *label;
                best_count = *count;
            }
            Ordering::Equal => {
                if *label < best {
                    best = *label;
                }
            }
            Ordering::Less => {}
        }
    }
    best
}

/// Most-frequent label among the neighbour labels collected in `scratch`
/// (UNKNOWN entries must already be excluded by the caller), with ties broken
/// towards the smallest label. `scratch` is sorted in place. This is the
/// allocation- and hash-free equivalent of [`majority_label`]: scanning the
/// sorted runs in ascending label order and only replacing the best on a
/// strictly-greater count reproduces the "largest count, smallest label on a
/// tie" semantics exactly. Returns `current` when `scratch` is empty.
pub fn majority_label_sorted(scratch: &mut [u32], current: u32) -> u32 {
    if scratch.is_empty() {
        return current;
    }
    scratch.sort_unstable();
    let mut best = current;
    let mut best_count = 0usize;
    let mut i = 0;
    while i < scratch.len() {
        let label = scratch[i];
        let mut j = i + 1;
        while j < scratch.len() && scratch[j] == label {
            j += 1;
        }
        let count = j - i;
        if count > best_count {
            best = label;
            best_count = count;
        }
        i = j;
    }
    best
}

/// Initialize label vector with semi-supervised seeds (or self-id in unsupervised mode).
pub fn init_labels(num_nodes: u32, initial_labels: &HashMap<u32, u32>) -> Vec<u32> {
    let mut labels = vec![UNKNOWN; num_nodes as usize];
    if initial_labels.is_empty() {
        for i in 0..num_nodes {
            labels[i as usize] = i;
        }
    } else {
        for (&node, &label) in initial_labels {
            if (node as usize) < labels.len() {
                labels[node as usize] = label;
            }
        }
    }
    labels
}

/// CSR-based label propagation. Canonical entry point reused by standalone,
/// rayon and mpi variants. The HashMap-based [`run_lp`] is a thin wrapper that
/// builds a [`CsrGraph`] and calls this.
pub fn run_lp_csr(
    csr: &CsrGraph,
    initial_labels: &HashMap<u32, u32>,
    max_iter: u32,
) -> Vec<u32> {
    let num_nodes = csr.num_nodes;
    let mut labels = init_labels(num_nodes, initial_labels);
    let unsupervised_mode = initial_labels.is_empty();

    let mut prev_labels = vec![UNKNOWN; num_nodes as usize];
    let mut scratch: Vec<u32> = Vec::new();

    for _ in 0..max_iter {
        prev_labels.copy_from_slice(&labels);
        let mut changed = 0;

        for i in 0..num_nodes {
            if !unsupervised_mode && initial_labels.contains_key(&i) {
                continue;
            }

            let current_label = prev_labels[i as usize];

            scratch.clear();
            for &neighbor in csr.neighbors(i) {
                let l = prev_labels[neighbor as usize];
                if l != UNKNOWN {
                    scratch.push(l);
                }
            }

            let new_label = majority_label_sorted(&mut scratch, current_label);

            if new_label != current_label {
                labels[i as usize] = new_label;
                changed += 1;
            }
        }

        if changed == 0 {
            break;
        }
    }
    labels
}

/// Backwards-compatible wrapper that builds a CSR on the fly and delegates to
/// [`run_lp_csr`]. New code (rayon, mpi, benchmark drivers) should construct
/// a [`CsrGraph`] once and call [`run_lp_csr`] directly to avoid the build cost.
pub fn run_lp(
    adj: &HashMap<u32, Vec<u32>>,
    initial_labels: &HashMap<u32, u32>,
    num_nodes: u32,
    max_iter: u32,
) -> Vec<u32> {
    let csr = build_csr_from_adj(adj, num_nodes);
    run_lp_csr(&csr, initial_labels, max_iter)
}

/// Represents a single node in the graph
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Node {
    /// Unique identifier for the node
    pub id: usize,
    /// Optional label (None for unlabeled nodes, Some(label) for labeled ones)
    pub label: Option<usize>,
    /// List of neighbor node IDs (unused in current implementation, adjacency map is used instead)
    pub neighbors: Vec<usize>,
}

/// Represents an undirected graph structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Graph {
    /// Vector of all nodes in the graph
    pub nodes: Vec<Node>,
    /// Adjacency list: maps node ID to list of neighbor IDs
    pub adjacency: HashMap<usize, Vec<usize>>,
}

/// Result of the label propagation algorithm
#[derive(Debug, Serialize, Deserialize)]
pub struct LabelPropagationResult {
    /// Final labels for all nodes (node_id -> label)
    pub labels: HashMap<usize, usize>,
    /// Number of iterations performed
    pub iterations: usize,
    /// Whether the algorithm converged before reaching max iterations
    pub converged: bool,
}

impl Graph {
    /// Creates a new empty graph
    pub fn new() -> Self {
        Graph {
            nodes: Vec::new(),
            adjacency: HashMap::new(),
        }
    }

    /// Adds a node to the graph
    ///
    /// # Arguments
    ///
    /// * `id` - Unique identifier for the node
    /// * `label` - Optional label (None for unlabeled, Some(label) for labeled)
    pub fn add_node(&mut self, id: usize, label: Option<usize>) {
        self.nodes.push(Node {
            id,
            label,
            neighbors: Vec::new(),
        });
        self.adjacency.insert(id, Vec::new());
    }

    /// Adds an undirected edge between two nodes
    ///
    /// # Arguments
    ///
    /// * `from` - Source node ID
    /// * `to` - Target node ID
    ///
    /// # Note
    ///
    /// Since the graph is undirected, this creates edges in both directions.
    /// Duplicate edges are prevented.
    pub fn add_edge(&mut self, from: usize, to: usize) {
        if let Some(neighbors) = self.adjacency.get_mut(&from) {
            if !neighbors.contains(&to) {
                neighbors.push(to);
            }
        }
        if let Some(neighbors) = self.adjacency.get_mut(&to) {
            if !neighbors.contains(&from) {
                neighbors.push(from);
            }
        }
    }

    /// Constructs a graph from an edge list and labeled nodes
    ///
    /// # Arguments
    ///
    /// * `edges` - Vector of tuples representing undirected edges
    /// * `labeled_nodes` - HashMap of node IDs to their initial labels
    ///
    /// # Returns
    ///
    /// A fully constructed Graph instance
    pub fn from_edge_list(edges: Vec<(usize, usize)>, labeled_nodes: HashMap<usize, usize>) -> Self {
        let mut graph = Graph::new();
        let mut node_ids = std::collections::HashSet::new();

        // Step 1: Collect all unique node IDs from the edge list
        for (from, to) in &edges {
            node_ids.insert(*from);
            node_ids.insert(*to);
        }

        // Step 2: Add all nodes to the graph with their labels (if any)
        for id in node_ids {
            let label = labeled_nodes.get(&id).copied();
            graph.add_node(id, label);
        }

        // Step 3: Add all edges to the graph
        for (from, to) in edges {
            graph.add_edge(from, to);
        }

        graph
    }
}

/// Semi-Supervised Label Propagation algorithm implementation
///
/// This implementation uses:
/// - **Synchronous updates**: All node labels are updated simultaneously in each iteration
/// - **Clamping**: Initial labeled nodes maintain their labels throughout
/// - **Deterministic tie-breaking**: When multiple labels have equal votes, the smallest label ID wins
/// - **Memory efficient**: Uses a change vector instead of cloning the entire label map
pub struct LabelPropagation {
    /// Maximum number of iterations before stopping
    max_iterations: usize,
    /// Convergence threshold: algorithm stops if change ratio falls below this value
    convergence_threshold: f64,
}

impl LabelPropagation {
    /// Creates a new LabelPropagation instance
    ///
    /// # Arguments
    ///
    /// * `max_iterations` - Maximum number of iterations (typical: 100)
    /// * `convergence_threshold` - Stop if less than this fraction of nodes change (typical: 0.01)
    pub fn new(max_iterations: usize, convergence_threshold: f64) -> Self {
        LabelPropagation {
            max_iterations,
            convergence_threshold,
        }
    }

    /// Runs the label propagation algorithm on a graph
    ///
    /// # Algorithm Overview
    ///
    /// 1. Initialize: Set labels for initially labeled nodes
    /// 2. Iterate:
    ///    - For each unlabeled node, count labels of its neighbors
    ///    - Assign the most common neighbor label (with deterministic tie-breaking)
    ///    - Apply all changes synchronously
    /// 3. Converge: Stop when change ratio < threshold or max iterations reached
    ///
    /// # Arguments
    ///
    /// * `graph` - The graph to propagate labels on
    ///
    /// # Returns
    ///
    /// A `LabelPropagationResult` containing final labels, iteration count, and convergence status
    pub fn propagate(&self, graph: &Graph) -> LabelPropagationResult {
        let mut labels: HashMap<usize, usize> = HashMap::new();
        
        // Initialize: Copy initial labels from seed nodes
        for node in &graph.nodes {
            if let Some(label) = node.label {
                labels.insert(node.id, label);
            }
        }

        let mut converged = false;
        let mut iterations = 0;

        // Store initial labeled nodes (clamping: these labels will never change)
        let initial_labels: HashMap<usize, usize> = graph
            .nodes
            .iter()
            .filter_map(|n| n.label.map(|l| (n.id, l)))
            .collect();

        // Main iteration loop
        for iter in 0..self.max_iterations {
            iterations = iter + 1;
            
            // Vector to store only pending changes (memory efficient: O(changes) instead of O(N))
            let mut pending_updates: Vec<(usize, usize)> = Vec::new();

            // Process each node in the graph
            for node in &graph.nodes {
                // Skip nodes with initial labels (clamping: seed nodes never change)
                if initial_labels.contains_key(&node.id) {
                    continue;
                }

                if let Some(neighbors) = graph.adjacency.get(&node.id) {
                    // Skip isolated nodes (no neighbors)
                    if neighbors.is_empty() {
                        continue;
                    }

                    // Count how many times each label appears in the neighborhood
                    let mut label_counts: HashMap<usize, usize> = HashMap::new();
                    
                    for neighbor_id in neighbors {
                        if let Some(&neighbor_label) = labels.get(neighbor_id) {
                            *label_counts.entry(neighbor_label).or_insert(0) += 1;
                        }
                    }

                    if !label_counts.is_empty() {
                        // Find the most common label with DETERMINISTIC tie-breaking
                        // This ensures reproducible results across multiple runs
                        // Tie-breaking rule: if counts are equal, prefer the SMALLER label ID
                        let most_common_label = label_counts
                            .iter()
                            .max_by(|a, b| {
                                let count_cmp = a.1.cmp(b.1);
                                if count_cmp == std::cmp::Ordering::Equal {
                                    // Tie-breaker: prefer smaller label ID (deterministic)
                                    b.0.cmp(a.0)
                                } else {
                                    count_cmp
                                }
                            })
                            .map(|(&label, _)| label)
                            .unwrap();

                        // Schedule update if the label would change
                        if labels.get(&node.id) != Some(&most_common_label) {
                            pending_updates.push((node.id, most_common_label));
                        }
                    }
                }
            }

            // Early convergence: if no changes, we're done
            if pending_updates.is_empty() {
                converged = true;
                break;
            }

            // Apply all pending changes SYNCHRONOUSLY (all at once)
            // This ensures deterministic behavior and prevents oscillations
            let changes = pending_updates.len();
            for (id, new_label) in pending_updates {
                labels.insert(id, new_label);
            }

            // Check convergence based on change ratio
            let total_unlabeled = graph.nodes.len() - initial_labels.len();
            let change_ratio = if total_unlabeled > 0 {
                changes as f64 / total_unlabeled as f64
            } else {
                0.0
            };

            // Converged if change ratio is below threshold
            if change_ratio < self.convergence_threshold {
                converged = true;
                break;
            }
        }

        LabelPropagationResult {
            labels,
            iterations,
            converged,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_propagation() {
        let edges = vec![(0, 1), (1, 2), (2, 3)];
        let mut labeled = HashMap::new();
        labeled.insert(0, 1);
        labeled.insert(3, 2);

        let graph = Graph::from_edge_list(edges, labeled);
        let lp = LabelPropagation::new(10, 0.01);
        let result = lp.propagate(&graph);

        assert!(result.labels.contains_key(&0));
        assert!(result.labels.contains_key(&3));
    }

    #[test]
    fn test_fully_labeled() {
        let edges = vec![(0, 1), (1, 2)];
        let mut labeled = HashMap::new();
        labeled.insert(0, 1);
        labeled.insert(1, 1);
        labeled.insert(2, 1);

        let graph = Graph::from_edge_list(edges, labeled);
        let lp = LabelPropagation::new(10, 0.01);
        let result = lp.propagate(&graph);

        assert_eq!(result.labels.len(), 3);
        assert!(result.converged);
    }

    #[test]
    fn test_triangle_graph() {
        // Triangle: todos conectados 0-1-2-0
        let mut adj = HashMap::new();
        adj.insert(0, vec![1, 2]);
        adj.insert(1, vec![0, 2]);
        adj.insert(2, vec![0, 1]);

        let mut initial_labels = HashMap::new();
        initial_labels.insert(0, 5);

        let result = run_lp(&adj, &initial_labels, 3, 10);

        assert_eq!(result[0], 5, "Node 0 should keep seed label 5");
        assert_eq!(result[1], 5, "Node 1 should adopt label 5");
        assert_eq!(result[2], 5, "Node 2 should adopt label 5");
    }

    #[test]
    fn test_star_graph() {
        // Star: Centro (0) conectado a 4 radios
        let mut adj = HashMap::new();
        adj.insert(0, vec![1, 2, 3, 4]);
        adj.insert(1, vec![0]);
        adj.insert(2, vec![0]);
        adj.insert(3, vec![0]);
        adj.insert(4, vec![0]);

        let mut initial_labels = HashMap::new();
        initial_labels.insert(0, 10);

        let result = run_lp(&adj, &initial_labels, 5, 10);

        assert_eq!(result[0], 10, "Center should keep label 10");
        assert_eq!(result[1], 10, "Spoke 1 should adopt label 10");
        assert_eq!(result[2], 10, "Spoke 2 should adopt label 10");
        assert_eq!(result[3], 10, "Spoke 3 should adopt label 10");
        assert_eq!(result[4], 10, "Spoke 4 should adopt label 10");
    }

    #[test]
    fn test_line_graph() {
        // Line: 0-1-2-3-4 con seeds en extremos
        let mut adj = HashMap::new();
        adj.insert(0, vec![1]);
        adj.insert(1, vec![0, 2]);
        adj.insert(2, vec![1, 3]);
        adj.insert(3, vec![2, 4]);
        adj.insert(4, vec![3]);

        let mut initial_labels = HashMap::new();
        initial_labels.insert(0, 100);
        initial_labels.insert(4, 200);

        let result = run_lp(&adj, &initial_labels, 5, 10);

        assert_eq!(result[0], 100, "Node 0 should keep seed label 100");
        assert_eq!(result[4], 200, "Node 4 should keep seed label 200");
        
        // Nodos intermedios deben tener una de las dos etiquetas
        assert!(result[1] == 100 || result[1] == 200);
        assert!(result[2] == 100 || result[2] == 200);
        assert!(result[3] == 100 || result[3] == 200);
    }

    #[test]
    fn test_disconnected_components() {
        // Dos componentes separados
        let mut adj = HashMap::new();
        adj.insert(0, vec![1, 2]);
        adj.insert(1, vec![0, 2]);
        adj.insert(2, vec![0, 1]);
        adj.insert(3, vec![4, 5]);
        adj.insert(4, vec![3, 5]);
        adj.insert(5, vec![3, 4]);

        let mut initial_labels = HashMap::new();
        initial_labels.insert(0, 10);
        initial_labels.insert(3, 20);

        let result = run_lp(&adj, &initial_labels, 6, 10);

        // Componente 1: todos deben ser 10
        assert_eq!(result[0], 10);
        assert_eq!(result[1], 10);
        assert_eq!(result[2], 10);

        // Componente 2: todos deben ser 20
        assert_eq!(result[3], 20);
        assert_eq!(result[4], 20);
        assert_eq!(result[5], 20);
    }

    #[test]
    fn test_unsupervised_mode() {
        // Modo no supervisado: sin seeds
        let mut adj = HashMap::new();
        adj.insert(0, vec![1, 2]);
        adj.insert(1, vec![0, 2]);
        adj.insert(2, vec![0, 1]);

        let initial_labels = HashMap::new();

        let result = run_lp(&adj, &initial_labels, 3, 10);

        // Todos convergen a la etiqueta más pequeña (0)
        assert_eq!(result[0], 0);
        assert_eq!(result[1], 0);
        assert_eq!(result[2], 0);
    }

    #[test]
    fn test_determinism() {
        // Ejecutar dos veces debe producir resultados idénticos
        let mut adj = HashMap::new();
        adj.insert(0, vec![1, 2, 3]);
        adj.insert(1, vec![0, 2]);
        adj.insert(2, vec![0, 1, 3]);
        adj.insert(3, vec![0, 2]);

        let mut initial_labels = HashMap::new();
        initial_labels.insert(0, 7);

        let result1 = run_lp(&adj, &initial_labels, 4, 10);
        let result2 = run_lp(&adj, &initial_labels, 4, 10);

        assert_eq!(result1, result2, "Algorithm should be deterministic");
    }

    #[test]
    fn test_convergence() {
        // Con suficientes iteraciones, resultado debe ser estable
        let mut adj = HashMap::new();
        adj.insert(0, vec![1, 2]);
        adj.insert(1, vec![0, 2]);
        adj.insert(2, vec![0, 1]);

        let mut initial_labels = HashMap::new();
        initial_labels.insert(0, 42);

        let result_few = run_lp(&adj, &initial_labels, 3, 3);
        let result_many = run_lp(&adj, &initial_labels, 3, 100);

        assert_eq!(result_few, result_many, "Should converge quickly");
    }

    #[test]
    fn test_csr_build_from_edges() {
        // Undirected triangle 0-1-2 — caller passes both directions
        let edges = vec![(0, 1), (1, 0), (1, 2), (2, 1), (0, 2), (2, 0)];
        let csr = build_csr_from_edges(3, &edges);
        assert_eq!(csr.num_nodes, 3);
        assert_eq!(csr.num_edges(), 6);
        let mut n0 = csr.neighbors(0).to_vec();
        n0.sort();
        assert_eq!(n0, vec![1, 2]);
        let mut n1 = csr.neighbors(1).to_vec();
        n1.sort();
        assert_eq!(n1, vec![0, 2]);
    }

    #[test]
    fn test_csr_lp_matches_hashmap_lp() {
        // Same graph via HashMap and CSR must yield identical labels.
        let mut adj: HashMap<u32, Vec<u32>> = HashMap::new();
        adj.insert(0, vec![1, 2]);
        adj.insert(1, vec![0, 2]);
        adj.insert(2, vec![0, 1]);
        adj.insert(3, vec![4, 5]);
        adj.insert(4, vec![3, 5]);
        adj.insert(5, vec![3, 4]);

        let mut seeds = HashMap::new();
        seeds.insert(0, 10);
        seeds.insert(3, 20);

        let csr = build_csr_from_adj(&adj, 6);
        let via_csr = run_lp_csr(&csr, &seeds, 10);
        let via_hash = run_lp(&adj, &seeds, 6, 10);
        assert_eq!(via_csr, via_hash);
    }

    #[test]
    fn test_tie_breaking() {
        // Test para verificar tie-breaking determinístico
        let mut adj = HashMap::new();
        adj.insert(0, vec![2]);
        adj.insert(1, vec![2]);
        adj.insert(2, vec![0, 1]);

        let mut initial_labels = HashMap::new();
        initial_labels.insert(0, 50);
        initial_labels.insert(1, 30);

        let result = run_lp(&adj, &initial_labels, 3, 5);

        assert_eq!(result[0], 50);
        assert_eq!(result[1], 30);
        // Node 2 tiene empate (1 voto para 50, 1 para 30)
        // Debe elegir la más pequeña (30)
        assert_eq!(result[2], 30, "Should break tie by choosing smallest label");
    }
}
