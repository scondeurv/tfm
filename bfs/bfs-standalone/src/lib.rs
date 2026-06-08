//! # BFS Standalone Library
//!
//! Standard queue-based Breadth-First Search for graph traversal benchmarking.
//! Companion to `ow-bfs` (distributed level-synchronous BFS).
//!
//! ## Properties
//!
//! - **Optimal**: O(N + E) time complexity via queue-based traversal
//! - **Deterministic**: same source always produces same levels
//! - **Unreachable nodes**: assigned level `UNVISITED` (u32::MAX)

use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};

/// Sentinel value for unvisited / unreachable nodes
pub const UNVISITED: u32 = u32::MAX;

/// Compressed Sparse Row representation of a directed graph.
/// `row_offsets[i]..row_offsets[i+1]` indexes into `dst` for node `i`'s out-edges.
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

/// Build CSR from a flat directed edge list. Callers must supply both
/// directions for undirected graphs.
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

/// Build CSR from a HashMap adjacency. Used by the legacy [`run_bfs`] wrapper.
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

/// Output of the BFS algorithm
#[derive(Debug, Serialize, Deserialize)]
pub struct BfsResult {
    /// BFS level (hop count) for each node; UNVISITED if unreachable from source
    pub levels: Vec<u32>,
    /// Number of nodes reachable from the source (including source itself)
    pub visited_nodes: u64,
    /// Maximum BFS level reached (diameter of the reachable subgraph)
    pub max_level: u32,
}

/// Canonical BFS over a [`CsrGraph`]. O(N + E) time, O(N) extra space.
pub fn run_bfs_csr(csr: &CsrGraph, source: u32, max_levels: u32) -> BfsResult {
    let n = csr.num_nodes as usize;
    let mut levels = vec![UNVISITED; n];

    if (source as usize) >= n {
        return BfsResult { levels, visited_nodes: 0, max_level: 0 };
    }

    levels[source as usize] = 0;
    let mut queue: VecDeque<u32> = VecDeque::new();
    queue.push_back(source);

    let mut visited_nodes: u64 = 1;
    let mut max_level: u32 = 0;

    while let Some(node) = queue.pop_front() {
        let current_level = levels[node as usize];
        if current_level >= max_levels {
            continue;
        }
        for &neighbor in csr.neighbors(node) {
            let idx = neighbor as usize;
            if idx < n && levels[idx] == UNVISITED {
                let new_level = current_level + 1;
                levels[idx] = new_level;
                if new_level > max_level {
                    max_level = new_level;
                }
                visited_nodes += 1;
                queue.push_back(neighbor);
            }
        }
    }

    BfsResult { levels, visited_nodes, max_level }
}

/// Backwards-compatible wrapper that builds CSR from the HashMap on the fly.
/// New code (rayon, mpi, benchmark drivers) should call [`run_bfs_csr`] directly.
pub fn run_bfs(
    adj: &HashMap<u32, Vec<u32>>,
    source: u32,
    num_nodes: u32,
    max_levels: u32,
) -> BfsResult {
    let csr = build_csr_from_adj(adj, num_nodes);
    run_bfs_csr(&csr, source, max_levels)
}

// ─────────────────────────────────────────── tests ───────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn make_adj(edges: &[(u32, u32)]) -> HashMap<u32, Vec<u32>> {
        let mut adj: HashMap<u32, Vec<u32>> = HashMap::new();
        for &(src, dst) in edges {
            adj.entry(src).or_default().push(dst);
        }
        adj
    }

    #[test]
    fn test_simple_chain() {
        // 0 → 1 → 2 → 3
        let adj = make_adj(&[(0, 1), (1, 2), (2, 3)]);
        let r = run_bfs(&adj, 0, 4, u32::MAX);
        assert_eq!(r.levels, [0, 1, 2, 3]);
        assert_eq!(r.visited_nodes, 4);
        assert_eq!(r.max_level, 3);
    }

    #[test]
    fn test_star_graph() {
        // 0 → 1, 2, 3, 4  (all at depth 1)
        let adj = make_adj(&[(0, 1), (0, 2), (0, 3), (0, 4)]);
        let r = run_bfs(&adj, 0, 5, u32::MAX);
        assert_eq!(r.levels[0], 0);
        for i in 1..5 {
            assert_eq!(r.levels[i], 1, "node {} should be at level 1", i);
        }
        assert_eq!(r.visited_nodes, 5);
        assert_eq!(r.max_level, 1);
    }

    #[test]
    fn test_disconnected() {
        // 0 → 1; node 2 isolated
        let adj = make_adj(&[(0, 1)]);
        let r = run_bfs(&adj, 0, 3, u32::MAX);
        assert_eq!(r.levels[0], 0);
        assert_eq!(r.levels[1], 1);
        assert_eq!(r.levels[2], UNVISITED, "node 2 unreachable");
        assert_eq!(r.visited_nodes, 2);
    }

    #[test]
    fn test_max_levels_cap() {
        // 0 → 1 → 2 → 3, but max_levels = 1
        let adj = make_adj(&[(0, 1), (1, 2), (2, 3)]);
        let r = run_bfs(&adj, 0, 4, 1);
        assert_eq!(r.levels[0], 0);
        assert_eq!(r.levels[1], 1);
        assert_eq!(r.levels[2], UNVISITED, "limited by max_levels");
        assert_eq!(r.levels[3], UNVISITED, "limited by max_levels");
    }

    #[test]
    fn test_undirected_triangle() {
        let adj = make_adj(&[(0, 1), (0, 2), (1, 0), (1, 2), (2, 0), (2, 1)]);
        let r = run_bfs(&adj, 0, 3, u32::MAX);
        assert_eq!(r.levels[0], 0);
        assert_eq!(r.levels[1], 1);
        assert_eq!(r.levels[2], 1);
        assert_eq!(r.visited_nodes, 3);
    }

    #[test]
    fn test_source_out_of_bounds() {
        let adj = make_adj(&[(0, 1), (1, 2)]);
        let r = run_bfs(&adj, 99, 3, u32::MAX);
        assert_eq!(r.visited_nodes, 0);
        assert!(r.levels.iter().all(|&l| l == UNVISITED));
    }

    #[test]
    fn test_different_source() {
        // 0 → 1 → 2 → 3; starting from 2
        let adj = make_adj(&[(0, 1), (1, 2), (2, 3)]);
        let r = run_bfs(&adj, 2, 4, u32::MAX);
        assert_eq!(r.levels[0], UNVISITED); // not reachable from 2
        assert_eq!(r.levels[1], UNVISITED);
        assert_eq!(r.levels[2], 0);
        assert_eq!(r.levels[3], 1);
        assert_eq!(r.visited_nodes, 2);
    }

    #[test]
    fn test_diamond_graph() {
        // 0 → 1, 0 → 2, 1 → 3, 2 → 3
        let adj = make_adj(&[(0, 1), (0, 2), (1, 3), (2, 3)]);
        let r = run_bfs(&adj, 0, 4, u32::MAX);
        assert_eq!(r.levels[0], 0);
        assert_eq!(r.levels[1], 1);
        assert_eq!(r.levels[2], 1);
        assert_eq!(r.levels[3], 2); // reached via either path, same level
        assert_eq!(r.visited_nodes, 4);
    }
}
