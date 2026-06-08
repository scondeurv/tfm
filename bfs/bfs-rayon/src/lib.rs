//! Rayon-based level-synchronous parallel BFS.
//!
//! Per-level loop: the current frontier (vertices discovered at level `L`) is
//! processed in parallel — each thread scans its slice's neighbours and tries
//! to claim each unvisited neighbour at level `L+1` via atomic
//! `compare_exchange`. Only the thread that wins the race appends `v` to the
//! next-frontier accumulator, guaranteeing `levels` is set exactly once per
//! node and the next frontier contains no duplicates.
//!
//! Compared to serial queue BFS this trades a tighter inner loop for the cost
//! of atomic CAS — wins on graphs where the frontier is large enough to
//! amortise the synchronisation overhead. For tiny frontiers the serial
//! version remains faster; we rely on the COST plot to delimit the regime.

use bfs_standalone::{BfsResult, CsrGraph, UNVISITED};
use rayon::prelude::*;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

pub fn run_bfs_rayon(csr: &CsrGraph, source: u32, max_levels: u32) -> BfsResult {
    let n = csr.num_nodes as usize;
    if (source as usize) >= n {
        return BfsResult {
            levels: vec![UNVISITED; n],
            visited_nodes: 0,
            max_level: 0,
        };
    }

    let levels: Vec<AtomicU32> = (0..n).map(|_| AtomicU32::new(UNVISITED)).collect();
    levels[source as usize].store(0, Ordering::Relaxed);

    let visited_nodes = AtomicU64::new(1);
    let mut max_level: u32 = 0;
    let mut current: Vec<u32> = vec![source];
    let mut level: u32 = 0;

    while !current.is_empty() && level < max_levels {
        level += 1;
        let new_level = level;

        let next: Vec<u32> = current
            .par_iter()
            .flat_map_iter(|&u| {
                let mut local: Vec<u32> = Vec::new();
                for &v in csr.neighbors(u) {
                    let vidx = v as usize;
                    if vidx >= n {
                        continue;
                    }
                    if levels[vidx]
                        .compare_exchange(
                            UNVISITED,
                            new_level,
                            Ordering::Relaxed,
                            Ordering::Relaxed,
                        )
                        .is_ok()
                    {
                        local.push(v);
                    }
                }
                local.into_iter()
            })
            .collect();

        if next.is_empty() {
            break;
        }

        visited_nodes.fetch_add(next.len() as u64, Ordering::Relaxed);
        max_level = new_level;
        current = next;
    }

    let levels: Vec<u32> = levels.into_iter().map(|a| a.into_inner()).collect();
    BfsResult {
        levels,
        visited_nodes: visited_nodes.load(Ordering::Relaxed),
        max_level,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bfs_standalone::{build_csr_from_edges, run_bfs_csr};

    #[test]
    fn rayon_matches_serial_on_chain() {
        let edges = vec![(0, 1), (1, 2), (2, 3)];
        let csr = build_csr_from_edges(4, &edges);
        let s = run_bfs_csr(&csr, 0, u32::MAX);
        let p = run_bfs_rayon(&csr, 0, u32::MAX);
        assert_eq!(s.levels, p.levels);
        assert_eq!(s.visited_nodes, p.visited_nodes);
        assert_eq!(s.max_level, p.max_level);
    }

    #[test]
    fn rayon_matches_serial_on_diamond() {
        let edges = vec![(0, 1), (0, 2), (1, 3), (2, 3)];
        let csr = build_csr_from_edges(4, &edges);
        let s = run_bfs_csr(&csr, 0, u32::MAX);
        let p = run_bfs_rayon(&csr, 0, u32::MAX);
        assert_eq!(s.levels, p.levels);
        assert_eq!(s.visited_nodes, p.visited_nodes);
    }

    #[test]
    fn rayon_respects_max_levels() {
        let edges = vec![(0, 1), (1, 2), (2, 3)];
        let csr = build_csr_from_edges(4, &edges);
        let p = run_bfs_rayon(&csr, 0, 1);
        assert_eq!(p.levels[0], 0);
        assert_eq!(p.levels[1], 1);
        assert_eq!(p.levels[2], UNVISITED);
        assert_eq!(p.levels[3], UNVISITED);
    }
}
