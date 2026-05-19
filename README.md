# TFM CloudLab Campaign Artifact

This archive contains the source code, scripts, tests, and prebuilt OpenWhisk
action ZIPs needed to run the CloudLab graph benchmark campaign.

The included campaign algorithms are:

- `labelpropagation`
- `bfs`
- `sssp`

The main campaign runner is:

```bash
python3 campaigns/run_cloudlab_campaign.py
```

The convenience launcher for the multi-algorithm campaign is:

```bash
bash campaigns/launch_full_campaign.sh
```

## Contents

- `campaigns/`: campaign orchestration scripts.
- `infra/`: shared runtime probes and resource-capacity helpers.
- `data_utils/`: dataset generation and upload helpers for included graph workloads.
- `openwhisk-deploy-kube-burst/`: OpenWhisk-on-Kubernetes deployment manifests/scripts used by the campaign.
- `burst-communication-middleware/`: Rust communication middleware used by Burst actions.
- `labelpropagation/`, `bfs/`, `sssp/`: algorithm implementations, benchmark drivers, Spark smoke scripts, and action packages.
- `tests/`: self-contained local regression tests.

Prebuilt action ZIPs included in the artifact:

- `labelpropagation/labelpropagation.zip`
- `labelpropagation/runtime_probe.zip`
- `bfs/bfs.zip`
- `sssp/sssp.zip`

## Test

From the unpacked package root:

```bash
./run_e2e_tests.sh all
```

These tests do not require MinIO, OpenWhisk, external validators, or standalone
algorithm crates.

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
full multi-algorithm launcher with `campaigns/launch_full_campaign.sh`.
