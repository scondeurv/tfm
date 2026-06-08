//! Rayon-based parallel synchronous Bellman-Ford. Shares the CSR + result
//! types from `sssp-standalone`.
//!
//! Strategy: store `dist` as `Vec<AtomicU32>` holding `f32::to_bits` values.
//! For non-negative `f32` values the IEEE-754 bit ordering matches numerical
//! ordering, so `AtomicU32::fetch_min` is a correct atomic distance update.
//! `INFINITY = 0x7f800000` (max finite-positive bit pattern + 1) so unvisited
//! slots compare strictly greater than any reachable distance.
//!
//! Each iteration `par_iter`s over the node range; every worker reads the
//! previous-iteration `prev` snapshot (immutable) and relaxes outgoing edges
//! directly into the atomic `dist`. No per-iteration allocation. The fold +
//! reduce merge step that the first prototype used was a bottleneck; the
//! atomic-bit version is the canonical parallel Bellman-Ford technique.

use rayon::prelude::*;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use sssp_standalone::{CsrGraph, SsspResult, INFINITY};

pub fn run_bellman_ford_rayon(
    csr: &CsrGraph,
    source: u32,
    max_iterations: u32,
) -> SsspResult {
    let n = csr.num_nodes as usize;
    if (source as usize) >= n {
        return SsspResult {
            distances: vec![INFINITY; n],
            reachable_nodes: 0,
            max_distance: 0.0,
        };
    }

    let inf_bits = INFINITY.to_bits();
    let dist: Vec<AtomicU32> = (0..n).map(|_| AtomicU32::new(inf_bits)).collect();
    dist[source as usize].store(0.0f32.to_bits(), Ordering::Relaxed);

    let mut prev = vec![INFINITY; n];

    for _ in 0..max_iterations {
        for (slot, atomic) in prev.iter_mut().zip(dist.iter()) {
            *slot = f32::from_bits(atomic.load(Ordering::Relaxed));
        }

        let changed = AtomicU64::new(0);
        (0..csr.num_nodes).into_par_iter().for_each(|u| {
            let pu = prev[u as usize];
            if !pu.is_finite() {
                return;
            }
            let (dst_slice, weight_slice) = csr.neighbors(u);
            for (k, &v) in dst_slice.iter().enumerate() {
                let vidx = v as usize;
                if vidx >= n {
                    continue;
                }
                let candidate = pu + weight_slice[k];
                if candidate.is_sign_negative() {
                    continue;
                }
                let cand_bits = candidate.to_bits();
                let prev_bits = dist[vidx].fetch_min(cand_bits, Ordering::Relaxed);
                if cand_bits < prev_bits {
                    changed.fetch_add(1, Ordering::Relaxed);
                }
            }
        });

        if changed.load(Ordering::Relaxed) == 0 {
            break;
        }
    }

    let final_dist: Vec<f32> = dist
        .into_iter()
        .map(|a| f32::from_bits(a.into_inner()))
        .collect();

    let reachable = final_dist.iter().filter(|d| d.is_finite()).count() as u64;
    let max_dist = final_dist
        .iter()
        .filter(|d| d.is_finite())
        .cloned()
        .fold(0.0f32, f32::max);

    SsspResult { distances: final_dist, reachable_nodes: reachable, max_distance: max_dist }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sssp_standalone::{build_csr_from_edges, run_bellman_ford_csr, MAX_ITERATIONS};

    #[test]
    fn rayon_matches_serial_on_diamond() {
        let edges = vec![(0, 1, 1.0), (0, 2, 5.0), (1, 3, 2.0), (2, 3, 1.0)];
        let csr = build_csr_from_edges(4, &edges);
        let serial = run_bellman_ford_csr(&csr, 0, MAX_ITERATIONS);
        let parallel = run_bellman_ford_rayon(&csr, 0, MAX_ITERATIONS);
        assert_eq!(serial.distances, parallel.distances);
        assert_eq!(serial.reachable_nodes, parallel.reachable_nodes);
    }

    #[test]
    fn rayon_matches_serial_on_undirected_triangle() {
        let edges = vec![
            (0, 1, 3.0), (0, 2, 1.0),
            (1, 0, 3.0), (1, 2, 1.0),
            (2, 0, 1.0), (2, 1, 1.0),
        ];
        let csr = build_csr_from_edges(3, &edges);
        let serial = run_bellman_ford_csr(&csr, 0, MAX_ITERATIONS);
        let parallel = run_bellman_ford_rayon(&csr, 0, MAX_ITERATIONS);
        assert_eq!(serial.distances, parallel.distances);
    }
}
