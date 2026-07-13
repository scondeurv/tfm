//! Rayon-based parallel LP. Shares the CSR + majority_label core with `lpst`.
//!
//! Differs from `lpst::run_lp_csr` in one place: the per-iteration node loop
//! runs as `par_iter` over `0..num_nodes`, producing a vector of `(node, new_label)`
//! updates that are then applied serially. `prev_labels` and the CSR are
//! shared read-only across threads — no synchronisation needed.

use label_propagation::{majority_label_sorted, CsrGraph, UNKNOWN, init_labels};
use rayon::prelude::*;
use std::collections::HashMap;

pub fn run_lp_rayon(
    csr: &CsrGraph,
    initial_labels: &HashMap<u32, u32>,
    max_iter: u32,
) -> Vec<u32> {
    let num_nodes = csr.num_nodes;
    let mut labels = init_labels(num_nodes, initial_labels);
    let unsupervised_mode = initial_labels.is_empty();

    let mut prev_labels = vec![UNKNOWN; num_nodes as usize];

    for _ in 0..max_iter {
        prev_labels.copy_from_slice(&labels);

        let updates: Vec<(u32, u32)> = (0..num_nodes)
            .into_par_iter()
            .filter_map(|i| {
                if !unsupervised_mode && initial_labels.contains_key(&i) {
                    return None;
                }

                let current_label = prev_labels[i as usize];

                let neighbors = csr.neighbors(i);
                if neighbors.is_empty() {
                    return None;
                }

                // Hash-free majority vote: collect non-UNKNOWN neighbour labels
                // into a scratch buffer and reuse `majority_label_sorted`, matching
                // the standalone (`lpst`) and MPI (`lp-mpi`) kernels exactly so the
                // three native paradigms share one comparable per-node cost.
                let mut scratch: Vec<u32> = Vec::with_capacity(neighbors.len());
                for &neighbor in neighbors {
                    let l = prev_labels[neighbor as usize];
                    if l != UNKNOWN {
                        scratch.push(l);
                    }
                }

                let new_label = majority_label_sorted(&mut scratch, current_label);
                if new_label != current_label {
                    Some((i, new_label))
                } else {
                    None
                }
            })
            .collect();

        if updates.is_empty() {
            break;
        }

        for (i, l) in &updates {
            labels[*i as usize] = *l;
        }
    }

    labels
}

#[cfg(test)]
mod tests {
    use super::*;
    use label_propagation::{build_csr_from_adj, run_lp_csr};

    #[test]
    fn rayon_matches_serial_on_two_components() {
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
        let serial = run_lp_csr(&csr, &seeds, 10);
        let parallel = run_lp_rayon(&csr, &seeds, 10);
        assert_eq!(serial, parallel);
    }

    #[test]
    fn rayon_unsupervised_deterministic() {
        // Two runs of the unsupervised path must match.
        let mut adj: HashMap<u32, Vec<u32>> = HashMap::new();
        adj.insert(0, vec![1, 2, 3]);
        adj.insert(1, vec![0, 2]);
        adj.insert(2, vec![0, 1, 3]);
        adj.insert(3, vec![0, 2]);
        let seeds = HashMap::new();
        let csr = build_csr_from_adj(&adj, 4);
        let r1 = run_lp_rayon(&csr, &seeds, 10);
        let r2 = run_lp_rayon(&csr, &seeds, 10);
        assert_eq!(r1, r2);
    }
}
