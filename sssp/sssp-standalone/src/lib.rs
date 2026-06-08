//! # Single-Source Shortest Path (SSSP) Standalone Library
//!
//! Implements Bellman-Ford (iterative relaxation) for computing shortest paths
//! from a single source to all reachable nodes in a weighted directed graph.
//!
//! The algorithm mirrors the distributed version to enable a fair performance
//! comparison where the only variable is communication overhead.
//!
//! ## Algorithm
//!
//! Synchronous Bellman-Ford:
//!   1. Set dist[source] = 0, all others = +∞
//!   2. Snapshot current distances
//!   3. For each node u with finite dist, relax all edges (u, v, w) against snapshot
//!   4. If no distance improved → stop; else repeat
//!
//! ## Properties
//!
//! - **Correct**: O(V·E) worst case, typically much fewer iterations on sparse graphs
//! - **Synchronous**: reads previous-iteration distances (matches distributed reduce semantics)
//! - **Non-negative weights required**: negative-weight cycles would loop indefinitely
//! - **Unreachable nodes**: assigned distance `f32::INFINITY`

use serde::{Deserialize, Serialize};

/// Sentinel value for unreachable nodes
pub const INFINITY: f32 = f32::INFINITY;

/// Maximum relaxation iterations (matches distributed default)
pub const MAX_ITERATIONS: u32 = 500;

/// CSR representation of a weighted directed graph.
/// `row_offsets[i]..row_offsets[i+1]` indexes into `dst`/`weights` for node `i`.
#[derive(Debug, Clone)]
pub struct CsrGraph {
    pub num_nodes: u32,
    pub row_offsets: Vec<u32>,
    pub dst: Vec<u32>,
    pub weights: Vec<f32>,
}

impl CsrGraph {
    pub fn neighbors(&self, node: u32) -> (&[u32], &[f32]) {
        let i = node as usize;
        let start = self.row_offsets[i] as usize;
        let end = self.row_offsets[i + 1] as usize;
        (&self.dst[start..end], &self.weights[start..end])
    }

    pub fn num_edges(&self) -> usize {
        self.dst.len()
    }
}

/// Build CSR from a flat weighted edge list `(src, dst, weight)`.
pub fn build_csr_from_edges(num_nodes: u32, edges: &[(u32, u32, f32)]) -> CsrGraph {
    let n = num_nodes as usize;
    let mut row_offsets = vec![0u32; n + 1];
    for &(src, _, _) in edges {
        if (src as usize) < n {
            row_offsets[src as usize + 1] += 1;
        }
    }
    for i in 1..=n {
        row_offsets[i] += row_offsets[i - 1];
    }
    let total = row_offsets[n] as usize;
    let mut dst = vec![0u32; total];
    let mut weights = vec![0.0f32; total];
    let mut cursor = row_offsets.clone();
    for &(src, d, w) in edges {
        let i = src as usize;
        if i < n {
            let pos = cursor[i] as usize;
            dst[pos] = d;
            weights[pos] = w;
            cursor[i] += 1;
        }
    }
    CsrGraph { num_nodes, row_offsets, dst, weights }
}

/// Build CSR from a flat slice-of-Vec adjacency. Used by the legacy
/// [`run_bellman_ford`] wrapper.
pub fn build_csr_from_adj(adj: &[Vec<(u32, f32)>], num_nodes: u32) -> CsrGraph {
    let n = num_nodes as usize;
    let mut row_offsets = vec![0u32; n + 1];
    for i in 0..n.min(adj.len()) {
        row_offsets[i + 1] = adj[i].len() as u32;
    }
    for i in 1..=n {
        row_offsets[i] += row_offsets[i - 1];
    }
    let total = row_offsets[n] as usize;
    let mut dst = vec![0u32; total];
    let mut weights = vec![0.0f32; total];
    for i in 0..n.min(adj.len()) {
        let start = row_offsets[i] as usize;
        for (k, &(d, w)) in adj[i].iter().enumerate() {
            dst[start + k] = d;
            weights[start + k] = w;
        }
    }
    CsrGraph { num_nodes, row_offsets, dst, weights }
}

/// Output of the SSSP algorithm
#[derive(Debug, Serialize, Deserialize)]
pub struct SsspResult {
    /// Shortest distance from source to each node; INFINITY if unreachable
    pub distances: Vec<f32>,
    /// Number of nodes reachable from the source (including source itself)
    pub reachable_nodes: u64,
    /// Maximum finite distance found
    pub max_distance: f32,
}

/// Canonical synchronous Bellman-Ford over a [`CsrGraph`].
pub fn run_bellman_ford_csr(
    csr: &CsrGraph,
    source: u32,
    max_iterations: u32,
) -> SsspResult {
    let n = csr.num_nodes as usize;
    let mut dist = vec![INFINITY; n];

    if (source as usize) >= n {
        return SsspResult { distances: dist, reachable_nodes: 0, max_distance: 0.0 };
    }

    dist[source as usize] = 0.0;
    let mut prev = vec![INFINITY; n];

    for _ in 0..max_iterations {
        prev.copy_from_slice(&dist);
        let mut changed = false;

        for u in 0..csr.num_nodes {
            if !prev[u as usize].is_finite() {
                continue;
            }
            let (dst_slice, weight_slice) = csr.neighbors(u);
            for (k, &v) in dst_slice.iter().enumerate() {
                let vidx = v as usize;
                if vidx >= n {
                    continue;
                }
                let candidate = prev[u as usize] + weight_slice[k];
                if candidate < dist[vidx] {
                    dist[vidx] = candidate;
                    changed = true;
                }
            }
        }

        if !changed {
            break;
        }
    }

    let reachable = dist.iter().filter(|d| d.is_finite()).count() as u64;
    let max_dist = dist
        .iter()
        .filter(|d| d.is_finite())
        .cloned()
        .fold(0.0f32, f32::max);

    SsspResult { distances: dist, reachable_nodes: reachable, max_distance: max_dist }
}

/// Backwards-compatible wrapper that builds CSR from `Vec<Vec<...>>` on the fly.
/// New code should call [`run_bellman_ford_csr`] directly to avoid the build cost.
pub fn run_bellman_ford(
    adj: &[Vec<(u32, f32)>],
    source: u32,
    num_nodes: u32,
    max_iterations: u32,
) -> SsspResult {
    let n = num_nodes as usize;
    assert!(adj.len() >= n, "adj size mismatch");
    let csr = build_csr_from_adj(adj, num_nodes);
    run_bellman_ford_csr(&csr, source, max_iterations)
}

// ─────────────────────────────────────── tests ───────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn make_adj(num_nodes: u32, edges: &[(u32, u32, f32)]) -> Vec<Vec<(u32, f32)>> {
        let mut adj = vec![Vec::new(); num_nodes as usize];
        for &(src, dst, w) in edges {
            adj[src as usize].push((dst, w));
        }
        adj
    }

    fn bf(adj: &[Vec<(u32, f32)>], source: u32) -> SsspResult {
        run_bellman_ford(adj, source, adj.len() as u32, MAX_ITERATIONS)
    }

    #[test]
    fn test_simple_chain() {
        // 0 -1.0-> 1 -2.0-> 2 -3.0-> 3
        let adj = make_adj(4, &[(0, 1, 1.0), (1, 2, 2.0), (2, 3, 3.0)]);
        let r = bf(&adj, 0);
        assert_eq!(r.distances[0], 0.0);
        assert_eq!(r.distances[1], 1.0);
        assert_eq!(r.distances[2], 3.0);
        assert_eq!(r.distances[3], 6.0);
        assert_eq!(r.reachable_nodes, 4);
        assert!((r.max_distance - 6.0).abs() < 1e-6);
    }

    #[test]
    fn test_star_graph() {
        // 0 -> 1(w=5), 0 -> 2(w=3), 0 -> 3(w=7), 0 -> 4(w=1)
        let adj = make_adj(
            5,
            &[(0, 1, 5.0), (0, 2, 3.0), (0, 3, 7.0), (0, 4, 1.0)],
        );
        let r = bf(&adj, 0);
        assert_eq!(r.distances[0], 0.0);
        assert_eq!(r.distances[1], 5.0);
        assert_eq!(r.distances[2], 3.0);
        assert_eq!(r.distances[3], 7.0);
        assert_eq!(r.distances[4], 1.0);
        assert_eq!(r.reachable_nodes, 5);
    }

    #[test]
    fn test_disconnected() {
        // 0 -> 1; node 2 isolated
        let adj = make_adj(3, &[(0, 1, 2.0)]);
        let r = bf(&adj, 0);
        assert_eq!(r.distances[0], 0.0);
        assert_eq!(r.distances[1], 2.0);
        assert!(r.distances[2].is_infinite());
        assert_eq!(r.reachable_nodes, 2);
    }

    #[test]
    fn test_diamond_shortest_path() {
        // 0 -> 1(w=1), 0 -> 2(w=5), 1 -> 3(w=2), 2 -> 3(w=1)
        // Shortest to 3: 0->1->3 = 3, not 0->2->3 = 6
        let adj = make_adj(4, &[(0, 1, 1.0), (0, 2, 5.0), (1, 3, 2.0), (2, 3, 1.0)]);
        let r = bf(&adj, 0);
        assert_eq!(r.distances[0], 0.0);
        assert_eq!(r.distances[1], 1.0);
        assert_eq!(r.distances[2], 5.0);
        assert_eq!(r.distances[3], 3.0); // via 0->1->3
        assert_eq!(r.reachable_nodes, 4);
    }

    #[test]
    fn test_undirected_triangle() {
        // Undirected: 0-1(w=3), 0-2(w=1), 1-2(w=1)
        // Shortest to 1: 0->2->1 = 2, not direct 0->1 = 3
        let adj = make_adj(
            3,
            &[
                (0, 1, 3.0),
                (0, 2, 1.0),
                (1, 0, 3.0),
                (1, 2, 1.0),
                (2, 0, 1.0),
                (2, 1, 1.0),
            ],
        );
        let r = bf(&adj, 0);
        assert_eq!(r.distances[0], 0.0);
        assert_eq!(r.distances[1], 2.0); // 0->2->1
        assert_eq!(r.distances[2], 1.0);
        assert_eq!(r.reachable_nodes, 3);
    }

    #[test]
    fn test_source_out_of_bounds() {
        let adj = make_adj(3, &[(0, 1, 1.0), (1, 2, 1.0)]);
        let r = run_bellman_ford(&adj, 99, 3, MAX_ITERATIONS);
        assert_eq!(r.reachable_nodes, 0);
        assert!(r.distances.iter().all(|&d| d.is_infinite()));
    }

    #[test]
    fn test_different_source() {
        // 0 -> 1 -> 2 -> 3; starting from 2
        let adj = make_adj(4, &[(0, 1, 1.0), (1, 2, 1.0), (2, 3, 1.0)]);
        let r = bf(&adj, 2);
        assert!(r.distances[0].is_infinite());
        assert!(r.distances[1].is_infinite());
        assert_eq!(r.distances[2], 0.0);
        assert_eq!(r.distances[3], 1.0);
        assert_eq!(r.reachable_nodes, 2);
    }

    #[test]
    fn test_parallel_paths() {
        // Two paths from 0 to 3:
        //   0 -10-> 1 -10-> 3  (total: 20)
        //   0 -1-> 2 -1-> 3    (total: 2)
        let adj = make_adj(4, &[(0, 1, 10.0), (1, 3, 10.0), (0, 2, 1.0), (2, 3, 1.0)]);
        let r = bf(&adj, 0);
        assert_eq!(r.distances[3], 2.0); // shorter path via 2
    }

    #[test]
    fn test_self_loop() {
        let adj = make_adj(3, &[(0, 0, 5.0), (0, 1, 1.0), (1, 2, 2.0)]);
        let r = bf(&adj, 0);
        assert_eq!(r.distances[0], 0.0); // self-loop doesn't affect source dist
        assert_eq!(r.distances[1], 1.0);
        assert_eq!(r.distances[2], 3.0);
    }

    #[test]
    fn test_zero_weight_edge() {
        let adj = make_adj(3, &[(0, 1, 0.0), (1, 2, 0.0)]);
        let r = bf(&adj, 0);
        assert_eq!(r.distances[0], 0.0);
        assert_eq!(r.distances[1], 0.0);
        assert_eq!(r.distances[2], 0.0);
        assert_eq!(r.reachable_nodes, 3);
    }

    #[test]
    fn test_large_weights() {
        let adj = make_adj(3, &[(0, 1, 1e10), (1, 2, 1e10)]);
        let r = bf(&adj, 0);
        assert_eq!(r.distances[0], 0.0);
        assert!((r.distances[1] - 1e10).abs() < 1.0);
        assert!((r.distances[2] - 2e10).abs() < 1.0);
    }

    #[test]
    fn test_single_node() {
        let adj = make_adj(1, &[]);
        let r = bf(&adj, 0);
        assert_eq!(r.distances[0], 0.0);
        assert_eq!(r.reachable_nodes, 1);
    }

    #[test]
    fn test_deterministic() {
        let adj = make_adj(
            4,
            &[(0, 1, 1.0), (0, 2, 5.0), (1, 3, 2.0), (2, 3, 1.0)],
        );
        let r1 = bf(&adj, 0);
        let r2 = bf(&adj, 0);
        assert_eq!(r1.distances, r2.distances);
        assert_eq!(r1.reachable_nodes, r2.reachable_nodes);
    }
}
