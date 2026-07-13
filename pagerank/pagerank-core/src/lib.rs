//! Canonical PageRank kernel + CSR builder shared across the standalone,
//! Rayon, MPI, and OpenWhisk backends.
//!
//! Convention: damping = 0.85, tolerance = 1e-6 (L1 norm of rank delta),
//! dangling-node mass distributed uniformly across all nodes per iteration.
//!
//! The graph format mirrors the LP/BFS/SSSP toolchain: directed edges in a
//! TSV file with `<src>\t<dst>` per line, vertices identified by `u32`.

use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;

/// Compressed Sparse Row representation, source-major.
///
/// `row_offsets[i]..row_offsets[i+1]` indexes into `out_neighbors` for vertex `i`.
/// `out_degree[i]` mirrors `row_offsets[i+1] - row_offsets[i]` for O(1) access.
pub struct Csr {
    pub num_nodes: u32,
    pub row_offsets: Vec<u32>,
    pub out_neighbors: Vec<u32>,
    pub out_degree: Vec<u32>,
}

impl Csr {
    pub fn num_edges(&self) -> usize {
        self.out_neighbors.len()
    }
}

/// Build a CSR from an in-memory edge list. Edges with source >= `num_nodes`
/// are silently dropped to tolerate stray IDs in synthetic datasets.
pub fn build_csr(num_nodes: u32, edges: &[(u32, u32)]) -> Csr {
    let n = num_nodes as usize;
    let mut out_degree = vec![0u32; n];
    for &(s, _) in edges {
        if (s as usize) < n {
            out_degree[s as usize] += 1;
        }
    }
    let mut row_offsets = vec![0u32; n + 1];
    let mut acc = 0u32;
    for i in 0..n {
        row_offsets[i] = acc;
        acc = acc.saturating_add(out_degree[i]);
    }
    row_offsets[n] = acc;
    let mut cursor = row_offsets.clone();
    let mut out_neighbors = vec![0u32; acc as usize];
    for &(s, d) in edges {
        if (s as usize) >= n {
            continue;
        }
        let slot = cursor[s as usize] as usize;
        out_neighbors[slot] = d;
        cursor[s as usize] += 1;
    }
    Csr {
        num_nodes,
        row_offsets,
        out_neighbors,
        out_degree,
    }
}

/// Read a directed TSV edge list at `path` into a `Vec<(u32, u32)>`.
pub fn load_edges<P: AsRef<Path>>(path: P) -> Vec<(u32, u32)> {
    let file = File::open(path).expect("cannot open graph file");
    let reader = BufReader::new(file);
    let mut edges = Vec::new();
    for line in reader.lines().flatten() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let mut parts = line.split_whitespace();
        if let (Some(s), Some(d)) = (parts.next(), parts.next()) {
            if let (Ok(s), Ok(d)) = (s.parse::<u32>(), d.parse::<u32>()) {
                edges.push((s, d));
            }
        }
    }
    edges
}

pub const DEFAULT_DAMPING: f32 = 0.85;
pub const DEFAULT_TOLERANCE: f32 = 1e-6;

/// One synchronous power-iteration step. Writes the contribution of each
/// vertex's rank into `next` (caller is responsible for zeroing it and folding
/// in the teleport + dangling mass).
///
/// For each non-dangling source vertex `i`, distributes `rank[i] / out_degree[i]`
/// to every out-neighbour.
pub fn power_iter_contribute(
    csr: &Csr,
    rank: &[f32],
    next: &mut [f32],
) -> f32 {
    let mut dangling_mass = 0.0f32;
    for i in 0..csr.num_nodes as usize {
        let deg = csr.out_degree[i];
        if deg == 0 {
            dangling_mass += rank[i];
            continue;
        }
        let share = rank[i] / deg as f32;
        let start = csr.row_offsets[i] as usize;
        let end = csr.row_offsets[i + 1] as usize;
        for k in start..end {
            next[csr.out_neighbors[k] as usize] += share;
        }
    }
    dangling_mass
}

/// Transposed (destination-major) CSR: for each vertex `v`,
/// `in_offsets[v]..in_offsets[v+1]` indexes `in_neighbors` listing the source
/// vertices `u` with an edge `u -> v`. Enables a contention-free pull-style
/// parallel contribute (each destination sums its in-neighbours independently).
pub struct CsrTranspose {
    pub in_offsets: Vec<u32>,
    pub in_neighbors: Vec<u32>,
}

/// Build the transpose of a source-major CSR. O(n + m), single pass over edges.
pub fn build_csr_transpose(csr: &Csr) -> CsrTranspose {
    let n = csr.num_nodes as usize;
    let mut in_degree = vec![0u32; n];
    for &v in &csr.out_neighbors {
        if (v as usize) < n {
            in_degree[v as usize] += 1;
        }
    }
    let mut in_offsets = vec![0u32; n + 1];
    let mut acc = 0u32;
    for v in 0..n {
        in_offsets[v] = acc;
        acc = acc.saturating_add(in_degree[v]);
    }
    in_offsets[n] = acc;
    let mut cursor = in_offsets.clone();
    let mut in_neighbors = vec![0u32; acc as usize];
    for u in 0..n {
        let start = csr.row_offsets[u] as usize;
        let end = csr.row_offsets[u + 1] as usize;
        for k in start..end {
            let v = csr.out_neighbors[k] as usize;
            if v < n {
                let slot = cursor[v] as usize;
                in_neighbors[slot] = u as u32;
                cursor[v] += 1;
            }
        }
    }
    CsrTranspose { in_offsets, in_neighbors }
}

/// Serial PageRank power iteration with damping + dangling redistribution.
/// Returns the converged rank vector (sums to 1.0).
pub fn run_pagerank(
    csr: &Csr,
    max_iter: u32,
    damping: f32,
    tolerance: f32,
) -> (Vec<f32>, u32) {
    let n = csr.num_nodes as usize;
    let mut rank = vec![1.0f32 / n as f32; n];
    let mut next = vec![0.0f32; n];
    let teleport_base = (1.0 - damping) / n as f32;
    for it in 0..max_iter {
        for x in next.iter_mut() {
            *x = 0.0;
        }
        let dangling = power_iter_contribute(csr, &rank, &mut next);
        let dangling_per_node = dangling / n as f32;
        let mut delta = 0.0f32;
        for i in 0..n {
            let new_v = teleport_base + damping * (next[i] + dangling_per_node);
            delta += (new_v - rank[i]).abs();
            rank[i] = new_v;
        }
        if delta < tolerance {
            return (rank, it + 1);
        }
    }
    (rank, max_iter)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pagerank_chain_distributes_mass() {
        // 0 → 1 → 2 → 0 (cycle of 3)
        let csr = build_csr(3, &[(0, 1), (1, 2), (2, 0)]);
        let (rank, iters) = run_pagerank(&csr, 100, 0.85, 1e-6);
        // Symmetric cycle → uniform stationary.
        assert!(iters > 0);
        for r in &rank {
            assert!((r - 1.0 / 3.0).abs() < 1e-3, "rank {r}");
        }
    }

    #[test]
    fn pagerank_dangling_node_redistribution() {
        // 0 → 1 ; 2 dangling
        let csr = build_csr(3, &[(0, 1)]);
        let (rank, _) = run_pagerank(&csr, 200, 0.85, 1e-7);
        let sum: f32 = rank.iter().sum();
        assert!((sum - 1.0).abs() < 1e-3, "ranks must sum to 1, got {sum}");
    }
}
