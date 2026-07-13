//! MPI-based LP. Mirrors Burst's "reduce by min" semantics:
//!
//! - Every rank holds the full CSR + full label vector (replicated graph).
//! - Per iteration, each rank computes new labels only for its owned node range.
//!   Non-owned nodes are written as `UNKNOWN` so they sort last under MIN.
//! - `Allreduce(MIN)` merges the local proposals across ranks — for any node
//!   exactly one rank owns it, so MIN selects that rank's value.
//! - Seeds are clamped on every rank identically.
//! - Convergence: `Allreduce(SUM)` over per-rank changed counts.
//!
//! Caveat: replicating the graph means memory does not scale with rank count.
//! Acceptable for the TFM COST experiment up to n ~ 10M (≈ 0.5 GB CSR per
//! rank). For n > 10M a partitioned-graph variant is needed.

use label_propagation::{majority_label_sorted, CsrGraph, UNKNOWN, init_labels};
use mpi::collective::SystemOperation;
use mpi::traits::*;
use std::collections::HashMap;

/// Compute the inclusive node range owned by `rank` out of `size` ranks for a
/// total of `num_nodes` nodes. Returns `(start, end)` where `end` is exclusive.
pub fn owned_range(num_nodes: u32, rank: i32, size: i32) -> (u32, u32) {
    let n = num_nodes as i32;
    let base = n / size;
    let rem = n % size;
    let start = rank * base + rank.min(rem);
    let end = start + base + if rank < rem { 1 } else { 0 };
    (start as u32, end as u32)
}

/// Run distributed LP over the given communicator. Returns the final label
/// vector on every rank (identical across ranks after the loop).
pub fn run_lp_mpi<C: Communicator>(
    world: &C,
    csr: &CsrGraph,
    initial_labels: &HashMap<u32, u32>,
    max_iter: u32,
) -> Vec<u32> {
    let rank = world.rank();
    let size = world.size();
    let num_nodes = csr.num_nodes;
    let n_usize = num_nodes as usize;
    let unsupervised_mode = initial_labels.is_empty();

    let mut labels = init_labels(num_nodes, initial_labels);
    let mut prev_labels = vec![UNKNOWN; n_usize];
    let mut proposed = vec![UNKNOWN; n_usize];
    let mut reduced = vec![UNKNOWN; n_usize];

    let (start, end) = owned_range(num_nodes, rank, size);

    for _ in 0..max_iter {
        prev_labels.copy_from_slice(&labels);

        // Initialise every slot to UNKNOWN; the rank that owns node i will
        // overwrite that slot with its real proposal. Allreduce(MIN) keeps
        // the smallest value, which (UNKNOWN being u32::MAX) picks the owner.
        for slot in proposed.iter_mut() {
            *slot = UNKNOWN;
        }
        let mut local_changed: u64 = 0;

        // Reusable scratch vector for neighbour-label collection. Cleared (not
        // dropped) between vertices: `Vec::clear` is O(len), not O(capacity),
        // unlike the HashMap pattern this replaces — which paid an O(capacity)
        // clear retained at the high-water mark set by the highest-degree hub
        // in the owned range. Equivalent semantics via `majority_label_sorted`.
        let mut scratch: Vec<u32> = Vec::new();
        for i in start..end {
            // Seed clamping — owners propose the seed value, never recompute.
            if !unsupervised_mode {
                if let Some(&seed) = initial_labels.get(&i) {
                    proposed[i as usize] = seed;
                    continue;
                }
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
            proposed[i as usize] = new_label;
            if new_label != current_label {
                local_changed += 1;
            }
        }

        world
            .all_reduce_into(&proposed[..], &mut reduced[..], SystemOperation::min());
        labels.copy_from_slice(&reduced);

        let mut global_changed: u64 = 0;
        world.all_reduce_into(&local_changed, &mut global_changed, SystemOperation::sum());
        if global_changed == 0 {
            break;
        }
    }

    labels
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
        // 10 / 3 = 3 rem 1 → rank 0 gets 4, ranks 1,2 get 3.
        let r0 = owned_range(10, 0, 3);
        let r1 = owned_range(10, 1, 3);
        let r2 = owned_range(10, 2, 3);
        assert_eq!(r0, (0, 4));
        assert_eq!(r1, (4, 7));
        assert_eq!(r2, (7, 10));
    }
}
