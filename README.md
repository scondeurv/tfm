# TFM CloudLab Campaign Artifact

This archive contains the source code, scripts, and tests needed to run the
CloudLab graph benchmark campaign. The OpenWhisk/Burst action packages are
compiled on demand by the per-algorithm `compile_*.sh` scripts and are not
committed to the repository.

The included campaign algorithms are:

- `labelpropagation`
- `bfs`
- `sssp`
- `pagerank`

The main campaign runner is:

```bash
python3 campaigns/run_cloudlab_campaign.py
```

The convenience launcher for the multi-algorithm campaign is:

```bash
bash campaigns/launch_campaign_v3.sh
```

To run the remaining heavy phases (size, cost, report) detached so they survive
a client disconnect:

```bash
setsid nohup ./run_rest_campaign.sh &
```

## Contents

- `campaigns/`: campaign orchestration scripts, preflight gate, cost backends,
  report generators, and implementation validators.
- `infra/`: shared runtime probes and resource-capacity helpers.
- `data_utils/`: dataset generation and upload helpers for the graph workloads.
- `openwhisk-deploy-kube-burst/`: OpenWhisk-on-Kubernetes deployment manifests/scripts used by the campaign.
- `burst-communication-middleware/`: Rust communication middleware used by Burst actions.
- `labelpropagation/`, `bfs/`, `sssp/`, `pagerank/`: algorithm implementations, benchmark drivers, and the `compile_*.sh` scripts that build each paradigm's binaries and action packages.
- `baselines/`: Apache Spark/GraphX jobs and deployment for the big-data tier.
- `tests/`: self-contained local regression tests.

The deployable Burst/OpenWhisk action packages (`*.zip`) are built from source
by the per-algorithm `compile_*.sh` scripts before deployment; they are not
stored in the repository.

## Build

The `standalone` and `rayon` tiers and the distributed `mpi` tier are Rust
crates. Build each with `cargo build --release` in its crate directory, e.g.:

```bash
(cd bfs/bfs-standalone && cargo build --release)
(cd bfs/bfs-rayon      && cargo build --release)
(cd bfs/bfs-mpi        && cargo build --release)
```

The Burst/OpenWhisk action packages are built by the per-algorithm
`compile_*.sh` scripts (e.g. `labelpropagation/compile_lp_cluster.sh`).

### MPI toolchain

The MPI crates use `rsmpi` (`mpi = "0.8"`), which builds against **OpenMPI 4.x
only** — it does *not* compile against OpenMPI 5.x, whose `MPI_Status` layout
changed. The campaign used OpenMPI 4.1.5. Point the build at a 4.1.x install:

```bash
export PATH=/path/to/openmpi-4.1.5/bin:$PATH
export LD_LIBRARY_PATH=/path/to/openmpi-4.1.5/lib
export MPICC=/path/to/openmpi-4.1.5/bin/mpicc
```

On a distribution whose system OpenMPI is 5.x, build 4.1.5 from source into a
local prefix (`--without-ofi` avoids a clash with a modern libfabric):

```bash
./configure --prefix=$HOME/opt/ompi415 --disable-mpi-fortran --without-ofi
make -j"$(nproc)" && make install
```

`rsmpi`'s `mpi-sys` pins `bindgen 0.69`, which fails to parse the MPI headers
with a very new `libclang` (≳ 18) and emits an opaque `MPI_Status`. If the MPI
crates fail with `no field 'MPI_SOURCE' on type 'ompi_status_public_t'`, point
bindgen at an older libclang (≤ 17):

```bash
pip install "libclang==16.0.6"
export LIBCLANG_PATH="$(python3 -c 'import clang.native,os; print(os.path.dirname(clang.native.__file__))')"
```

## Test

From the unpacked package root, the self-contained suite (no cluster, no
external services) is:

```bash
./run_e2e_tests.sh all
```

The oracle + property-based correctness suite additionally needs
NetworkX/Hypothesis in a dedicated venv and the built `standalone`/`rayon`/`mpi`
binaries; it then validates every backend against the NetworkX reference:

```bash
python3 -m venv tests/.venv-test
tests/.venv-test/bin/pip install -r tests/requirements-test.txt
./run_e2e_tests.sh correctness
```

The `all` suite needs neither MinIO/OpenWhisk nor external validators; the
`correctness` suite needs only the venv and built binaries shown above.

For algorithmic correctness of the Rust Burst cores:

```bash
./run_algorithm_correctness_tests.sh
```

This runs the deterministic Rust unit/distributed in-process tests with
`cargo test --offline`; it is separate from the campaign measurement path.

## Run

Set MinIO/S3 credentials before launching a campaign:

```bash
export AWS_ACCESS_KEY_ID=<minio-access-key>
export AWS_SECRET_ACCESS_KEY=<minio-secret-key>
```

Then run either a single phase with `campaigns/run_cloudlab_campaign.py` or the
full multi-algorithm launcher with `campaigns/launch_campaign_v3.sh`.
