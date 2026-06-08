//! MPI synchronous Bellman-Ford. Mirrors the partitioning scheme used by
//! `lp-mpi`:
//!
//! - All ranks hold the full CSR + full `dist` vector (replicated graph).
//! - Per iteration, each rank initialises `proposed = prev`, then relaxes
//!   outgoing edges only from nodes in its owned range.
//! - `Allreduce(MIN)` merges proposals: for every node `v`, the smallest
//!   candidate distance across all ranks wins.
//! - `Allreduce(SUM)` over per-rank changed counts decides termination.
//!
//! Caveat: replicating the graph means memory does not scale with rank count.
//! Acceptable for the TFM COST experiment up to n ~ 10M.

use mpi::collective::SystemOperation;
use mpi::traits::*;
use sssp_standalone::{CsrGraph, SsspResult, INFINITY};

pub fn owned_range(num_nodes: u32, rank: i32, size: i32) -> (u32, u32) {
    let n = num_nodes as i32;
    let base = n / size;
    let rem = n % size;
    let start = rank * base + rank.min(rem);
    let end = start + base + if rank < rem { 1 } else { 0 };
    (start as u32, end as u32)
}

pub fn run_bellman_ford_mpi<C: Communicator>(
    world: &C,
    csr: &CsrGraph,
    source: u32,
    max_iterations: u32,
) -> SsspResult {
    let rank = world.rank();
    let size = world.size();
    let num_nodes = csr.num_nodes;
    let n_usize = num_nodes as usize;

    let mut dist = vec![INFINITY; n_usize];
    if (source as usize) < n_usize {
        dist[source as usize] = 0.0;
    }
    let mut prev = vec![INFINITY; n_usize];
    let mut proposed = vec![INFINITY; n_usize];
    let mut reduced = vec![INFINITY; n_usize];

    let (start, end) = owned_range(num_nodes, rank, size);

    for _ in 0..max_iterations {
        prev.copy_from_slice(&dist);
        proposed.copy_from_slice(&prev);

        let mut local_changed: u64 = 0;
        for u in start..end {
            let pu = prev[u as usize];
            if !pu.is_finite() {
                continue;
            }
            let (dst_slice, weight_slice) = csr.neighbors(u);
            for (k, &v) in dst_slice.iter().enumerate() {
                let vidx = v as usize;
                if vidx >= n_usize {
                    continue;
                }
                let candidate = pu + weight_slice[k];
                if candidate < proposed[vidx] {
                    proposed[vidx] = candidate;
                    local_changed += 1;
                }
            }
        }

        world
            .all_reduce_into(&proposed[..], &mut reduced[..], SystemOperation::min());
        dist.copy_from_slice(&reduced);

        let mut global_changed: u64 = 0;
        world.all_reduce_into(&local_changed, &mut global_changed, SystemOperation::sum());
        if global_changed == 0 {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn owned_range_partitions_evenly() {
        let (s, e) = owned_range(100, 0, 4);
        assert_eq!((s, e), (0, 25));
        let (s, e) = owned_range(100, 3, 4);
        assert_eq!((s, e), (75, 100));
    }
}
