//! MPI level-synchronous BFS. Mirrors the partitioning scheme used by
//! `lp-mpi` and `sssp-mpi`:
//!
//! - All ranks hold the full CSR + full `levels` vector (replicated graph).
//! - Per level `L`, each rank scans the slice of nodes it owns whose
//!   `prev_levels[u] == L` and proposes `L+1` for their unvisited neighbours.
//!   Proposals are written into the local `levels` buffer using `min(current,
//!   L+1)` so concurrent writes within a rank are safe.
//! - `Allreduce(MIN)` over `levels` merges proposals across ranks.
//! - Termination: `Allreduce(SUM)` over per-rank newly-discovered counts.
//!
//! Caveat: replicated graph → memory does not scale with rank count. Fine for
//! the COST experiment up to n ~ 10M.

use bfs_standalone::{BfsResult, CsrGraph, UNVISITED};
use mpi::collective::SystemOperation;
use mpi::traits::*;

pub fn owned_range(num_nodes: u32, rank: i32, size: i32) -> (u32, u32) {
    let n = num_nodes as i32;
    let base = n / size;
    let rem = n % size;
    let start = rank * base + rank.min(rem);
    let end = start + base + if rank < rem { 1 } else { 0 };
    (start as u32, end as u32)
}

pub fn run_bfs_mpi<C: Communicator>(
    world: &C,
    csr: &CsrGraph,
    source: u32,
    max_levels: u32,
) -> BfsResult {
    let rank = world.rank();
    let size = world.size();
    let num_nodes = csr.num_nodes;
    let n_usize = num_nodes as usize;

    let mut levels = vec![UNVISITED; n_usize];
    if (source as usize) < n_usize {
        levels[source as usize] = 0;
    }
    let mut prev_levels = vec![UNVISITED; n_usize];
    let mut reduced = vec![UNVISITED; n_usize];

    let (start, end) = owned_range(num_nodes, rank, size);

    let mut current_level: u32 = 0;
    while current_level < max_levels {
        prev_levels.copy_from_slice(&levels);
        let next_level = current_level + 1;

        let mut local_added: u64 = 0;
        for u in start..end {
            if prev_levels[u as usize] != current_level {
                continue;
            }
            for &v in csr.neighbors(u) {
                let vidx = v as usize;
                if vidx >= n_usize {
                    continue;
                }
                if levels[vidx] > next_level {
                    levels[vidx] = next_level;
                    local_added += 1;
                }
            }
        }

        world
            .all_reduce_into(&levels[..], &mut reduced[..], SystemOperation::min());
        levels.copy_from_slice(&reduced);

        let mut global_added: u64 = 0;
        world.all_reduce_into(&local_added, &mut global_added, SystemOperation::sum());
        if global_added == 0 {
            break;
        }
        current_level = next_level;
    }

    let visited_nodes = levels.iter().filter(|&&l| l != UNVISITED).count() as u64;
    let max_level = levels
        .iter()
        .filter(|&&l| l != UNVISITED)
        .copied()
        .max()
        .unwrap_or(0);
    BfsResult { levels, visited_nodes, max_level }
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
