# Scripts — TFM v3 reproducibility

Dos entrypoints, propósitos distintos.

## `validate_implementations.sh` (local)

Determinism proof local. No CloudLab needed.

```bash
bash campaigns/validate_implementations.sh
```

Steps:

1. Compila `<algo>-standalone` y `<algo>-rayon` para LP, BFS, SSSP, PageRank.
2. Intenta compilar `<algo>-mpi`; salta gracefully si `rsmpi` no compila con OpenMPI local (típico OpenMPI 5.x; rsmpi 0.8 sólo funciona con OpenMPI 4.x).
3. Corre dos suites:
   - **Smoke** (`tests.test_cross_backend_correctness`): toy graphs (4–5 nodes) per algo. Aggregate checks.
   - **Full determinism proof** (`tests.test_determinism_proof`): 4 algos × 5 fixtures × 3 thread counts. **Full-vector** comparison:
     - **BFS** `levels[i]` exact equality (u32, `u32::MAX` for unreachable).
     - **SSSP** `distances[i]` bit-exact f32 (incluye `f32::INFINITY`).
     - **PageRank** `rank[i]` element-wise ε=1e-5.
     - **LP** partition equivalence (label IDs arbitrary; any consistent relabeling pasa).

Fixtures (`tests/determinism_fixtures.py`):

| Fixture | Tamaño | Para qué |
|---|---|---|
| `path_100` | 100 nodes / 99 edges | profundidad máxima BFS/SSSP |
| `star_50` | 50 nodes hub+49 sinks | dangling nodes PR |
| `two_components_50each` | 100 nodes / 2 chains disjuntos | unreachable BFS/SSSP |
| `er_1000_p01` | 1000 nodes, p=0.01, seed=42 | random dense-ish |
| `self_loops_20` | 20 nodes, self-loops + multi-edges | input patológico |

Exit code 0 = todas las suites pasan. Skip MPI local no es fallo.

## `validate_implementations_cluster.sh` (CloudLab)

Mismo set de tests pero ejecutado en compute6 contra OpenMPI 4.1.5. Cubre los 3 backends MPI (LP, BFS, SSSP, PageRank) que el host local no puede compilar.

```bash
bash campaigns/validate_implementations_cluster.sh
```

Pasos:

1. SSH preflight a `${CLOUDLAB_HOST}` (default `compute6`).
2. Rsync `tests/{determinism_*.py,test_determinism_proof.py,test_cross_backend_correctness.py}` y los sources `*-{standalone,rayon,mpi}/src/` al host.
3. `cargo build --release` para los 12 crates (LP/BFS/SSSP/PR × {standalone,rayon,mpi}) con `OPENMPI_DIR=${MPI_PREFIX}` exportado.
4. `python3 -m unittest tests.test_determinism_proof -v` remoto.

Override hosts/keys via env (mismo convention que `launch_campaign_v3.sh`):
`CLOUDLAB_HOST`, `CLOUDLAB_SSH_KEY`, `CLOUDLAB_SRC_ROOT`, `MPI_PREFIX`.

**Burst y Spark** no se re-validan aquí. Estos scripts solo prueban standalone,
Rayon y MPI. En `campaign-unified-20260529T092711Z`, los raw de tiempo llevan
`validation.performed=false`, así que no se debe inferir correctness de Burst ni
Spark a partir de estos logs. Si se requiere validación inline en una campaña
futura, hay que activar explícitamente `--validate` en los benchmarks
correspondientes.

## `launch_campaign_v3.sh`

Lanza la campaña unificada completa sobre CloudLab.

```bash
bash campaigns/launch_campaign_v3.sh
```

Defaults coinciden con `campaign-unified-20260524T064518Z`:

| Parámetro | Default |
|---|---|
| `BACKENDS` | `standalone,rayon,mpi,burst,spark` |
| `SIZE_NODES` | `100000,1000000,10000000` |
| `COST_SWEEP_NODES` | `10000,100000,1000000,10000000` |
| `BURST_PARTITIONS` | `4,8` |
| `SPARK_PARTITIONS` | `4` |
| `SPARK_EXECUTORS` | `4` |
| `RAYON_THREADS` | `1,4,16,32` |
| `MPI_RANKS` | `4,16,32` |
| `MPI_HOSTS` | `compute6:32,compute7:32` (cross-host) |
| `MPI_MAP_BY` | `node` (fuerza reparto entre nodos) |
| `MPI_BTL_IF_INCLUDE` | `192.168.5.0/24` (red 10 GbE; `.2` es el bridge K8s a 1 GbE) |
| `SIZE_RUNS` | `3` |
| `COST_RUNS` | `3` |
| `MAX_ITER` | `20` |
| `SPARK_CELL_TIMEOUT_SEC` | `5400` (corta celdas GraphX colgadas; importa con SIZE_RUNS>1) |
| `OW_HOST` | `10.100.204.130` (ClusterIP del nginx de OpenWhisk) |
| `OW_PORT` | `80` |
| `CLOUDLAB_HOST` | `compute6` (host al que se hace SSH; el resto de `MPI_HOSTS` reciben dataset+binario por propagación automática) |

Para reproducir la campaña intra-host original (un solo nodo), exporta
`MPI_HOSTS=compute6:64 MPI_MAP_BY=` (vacío).

Override cualquiera vía env var:

```bash
SIZE_NODES=100000,1000000 BURST_PARTITIONS=4 bash campaigns/launch_campaign_v3.sh
```

### Pre-requisitos del cluster (manuales)

El script asume que la infraestructura está ya levantada. Lo que **debe** estar listo antes:

1. **CloudLab nodes** `compute2/4/5/6/7` provisionados, Ubuntu 22.04 + K8s 1.28. SSH passwordless desde la máquina local vía `~/.ssh/id_pc1`.
2. **OpenWhisk Burst** desplegado con `BURST_STANDALONE=true` en controller e invoker, y `userMemory` bumped a 65536m:
   ```bash
   kubectl -n openwhisk set env statefulset/owdev-invoker \
     CONFIG_whisk_containerPool_userMemory=65536m
   kubectl -n openwhisk rollout status statefulset/owdev-invoker
   ```
3. **OpenMPI 4.1.5** construido user-space en `/home/users/sconde/opt/openmpi-4.1.5` **en cada nodo MPI** (compute6 y compute7; necesario porque rsmpi 0.8 no compila contra OpenMPI 5.x). El dataset y el binario `<algo>-mpi` NO necesitan compilarse en compute7: el orquestador los propaga automáticamente desde compute6 a cada host extra de `MPI_HOSTS` (vía `scp -p`, ya que `/home` es ext2/3, no NFS). SSH passwordless compute6→compute7 ya configurado (clave de compute6 en `authorized_keys` de compute7). Verificación rápida antes de lanzar:
   ```bash
   ssh compute6 '/home/users/sconde/opt/openmpi-4.1.5/bin/mpirun -np 4 \
     -H compute6:32,compute7:32 --map-by node \
     --mca btl tcp,self --mca btl_tcp_if_include 192.168.5.0/24 hostname'
   # esperado: 2x compute6 + 2x compute7
   ```
4. **Rust toolchain + libclang** en compute6 (`rustup default stable`, `apt install libclang-dev` ya en el imagen).
5. **Binarios COST compilados** en compute6:
   ```bash
   ssh sconde@cloudfunctions.urv.cat 'cd /home/users/sconde/src && \
     for algo in labelpropagation bfs sssp pagerank; do \
       bash $algo/compile_${algo%standalone}_cost_backends.sh; \
     done'
   ```
6. **Zips de acciones Burst** compilados localmente:
   ```bash
   bash labelpropagation/compile_lp_cluster.sh
   bash bfs/compile_bfs_cluster.sh
   bash sssp/compile_sssp_cluster.sh
   bash pagerank/compile_pagerank_cluster.sh
   ```
   El orquestador los sube al cluster por scp en cada celda Burst.
7. **MinIO con bucket `tfm-smoke`** accesible desde compute6. Credenciales en `.env` en la raíz del repo:
   ```
   AWS_ACCESS_KEY_ID=lab144
   AWS_SECRET_ACCESS_KEY=astl1a4b4
   S3_HOST_ENDPOINT=http://192.168.5.24:9000
   S3_WORKER_ENDPOINT=http://192.168.5.24:9000
   ```
8. **Manifiestos K8s de Spark** en `baselines/cloudlab/k8s/`. El cluster Spark se trae arriba idempotentemente al inicio de cada celda Spark vía `deploy-spark-smoke.sh`.

### Salida

Al terminar:

- `experiment_data/cloudlab_campaigns/campaign-unified-<TS>/raw_runs/` — JSON por celda.
- `…/size_sweep/<algo>_{burst,spark}_runs_p<P>.json` + `…_summary_p<P>.json`.
- `…/cost_sweep/runs_{standalone,rayon,mpi}.json` + `summary.json`.
- `…/report/<algo>/{cost_table,cross_backend_table,size_burst_table}.md` + `cost_loglog.png`, `cost_speedup.png`, `size_burst_vs_spark.png`.
- `…/report/summary.md` + `README.md` con metadata de la campaña.

### Wall time

Sobre el cluster CloudLab con la matriz default, la campaña tarda aproximadamente:

- COST sweep: ~2–3 h (standalone n=10M domina LP+SSSP).
- Burst sweep: ~1.5 h (24 celdas × ~3 min media).
- Spark sweep: ~5 h si incluyes n=10M para los 4 algos; ~1 h si limitas Spark a n≤1M (override `SPARK_SIZE_NODES`; ver §6.3 del informe para el caveat de scope).

Total realista: **~6–10 h sostenidas**.
