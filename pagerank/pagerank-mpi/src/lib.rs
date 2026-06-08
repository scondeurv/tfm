//! MPI PageRank. Mirrors the partitioning scheme used by `lp-mpi` /
//! `bfs-mpi` / `sssp-mpi`:
//!
//! - All ranks hold the full CSR + full `rank` vector (replicated graph).
//! - Per iteration, each rank scans the slice of source vertices it owns,
//!   accumulating contributions into a local `next` buffer of length `n`.
//! - `Allreduce(SUM)` merges contributions across ranks.
//! - The dangling-node mass is summed locally then `Allreduce(SUM)` to a
//!   global scalar.
//! - Each rank then applies teleport + damping + dangling redistribution
//!   identically (deterministic — no extra reduction needed).
//! - Termination: `Allreduce(SUM)` over per-rank delta accumulator vs
//!   tolerance.
//!
//! Caveat: replicated graph → memory does not scale with rank count.
//! Acceptable for the TFM COST experiment up to n ~ 10M (~250 MB for PR
//! since rank vector is f32 not the heavier LP label vector clone).

use mpi::collective::SystemOperation;
use mpi::traits::*;
use pagerank_core::Csr;

pub fn owned_range(num_nodes: u32, rank: i32, size: i32) -> (u32, u32) {
    let n = num_nodes as i32;
    let base = n / size;
    let rem = n % size;
    let start = rank * base + rank.min(rem);
    let end = start + base + if rank < rem { 1 } else { 0 };
    (start as u32, end as u32)
}

pub struct PageRankMpiResult {
    pub rank: Vec<f32>,
    pub iterations: u32,
    pub max_rank: f32,
    pub sum_rank: f64,
}

pub fn run_pagerank_mpi<C: Communicator>(
    world: &C,
    csr: &Csr,
    max_iter: u32,
    damping: f32,
    tolerance: f32,
) -> PageRankMpiResult {
    let rank_id = world.rank();
    let size = world.size();
    let n = csr.num_nodes as usize;

    let mut rank_vec = vec![1.0f32 / n as f32; n];
    let mut local_next = vec![0.0f32; n];
    let mut global_next = vec![0.0f32; n];
    let teleport_base = (1.0 - damping) / n as f32;

    let (start, end) = owned_range(csr.num_nodes, rank_id, size);

    let mut iters_done: u32 = 0;
    for it in 0..max_iter {
        for x in local_next.iter_mut() {
            *x = 0.0;
        }

        let mut local_dangling: f32 = 0.0;
        for u in start..end {
            let uidx = u as usize;
            let deg = csr.out_degree[uidx];
            if deg == 0 {
                local_dangling += rank_vec[uidx];
                continue;
            }
            let share = rank_vec[uidx] / deg as f32;
            let s = csr.row_offsets[uidx] as usize;
            let e = csr.row_offsets[uidx + 1] as usize;
            for k in s..e {
                let v = csr.out_neighbors[k] as usize;
                if v < n {
                    local_next[v] += share;
                }
            }
        }

        world.all_reduce_into(&local_next[..], &mut global_next[..], SystemOperation::sum());
        let mut global_dangling: f32 = 0.0;
        world.all_reduce_into(&local_dangling, &mut global_dangling, SystemOperation::sum());

        let dangling_per_node = global_dangling / n as f32;
        let mut local_delta: f32 = 0.0;
        for i in 0..n {
            let new_v = teleport_base + damping * (global_next[i] + dangling_per_node);
            local_delta += (new_v - rank_vec[i]).abs();
            rank_vec[i] = new_v;
        }
        // Delta is computed identically on all ranks because each rank applies
        // the same update over the same `global_next + global_dangling`. No
        // extra reduction is needed for termination — every rank breaks at the
        // same iteration.
        iters_done = it + 1;
        if local_delta < tolerance {
            break;
        }
    }

    let max_rank = rank_vec.iter().cloned().fold(0.0f32, f32::max);
    let sum_rank: f64 = rank_vec.iter().map(|&r| r as f64).sum();
    PageRankMpiResult { rank: rank_vec, iterations: iters_done, max_rank, sum_rank }
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

    #[test]
    fn owned_range_handles_remainder() {
        let (s0, e0) = owned_range(10, 0, 3);
        let (s1, e1) = owned_range(10, 1, 3);
        let (s2, e2) = owned_range(10, 2, 3);
        assert_eq!((s0, e0), (0, 4));
        assert_eq!((s1, e1), (4, 7));
        assert_eq!((s2, e2), (7, 10));
    }
}
