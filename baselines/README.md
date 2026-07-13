# Spark Baseline

Baseline de Apache Spark para comparar contra las implementaciones `standalone`
y `burst` del TFM.

## Objetivo

Este baseline prioriza una configuración reproducible y sencilla:

- Spark en `docker compose`
- modo `standalone`
- `GraphX` como API base
- primer job listo: `SSSPGraphXJob`

Para la campaña comparable del TFM, el clúster local se ajusta a un presupuesto
operativo común de `4` CPU lógicas y `16 GiB` de RAM para Spark, alineado con
las campañas Burst de grafos (`4` particiones, `1` CPU por acción y `4096 MiB`
por worker en BFS/SSSP/Label Propagation).

## Estructura

- `docker-compose.yml`: cluster local `master + 4 workers`
- `jobs/`: proyecto `sbt` con jobs de Spark
- `scripts/build-job.sh`: compila el `.jar` si quieres pasar a aplicaciones empaquetadas
- `scripts/submit-sssp.sh`: benchmark de SSSP usando `spark-shell`
- `scripts/submit-bfs.sh`: benchmark de BFS usando `spark-shell`
- `scripts/submit-lp.sh`: benchmark de Label Propagation semisupervisado usando `spark-shell`
- `run_spark_graph_benchmarks.py`: runner agregado para lanzar la malla completa de Spark
- `data/`: datasets montados dentro de los contenedores
- `results/`: salidas de Spark

## Imagen de Spark

Usa una imagen oficial `spark` basada en Spark `3.5.x`.

La `docker-compose.yml` acepta una variable de entorno:

```bash
export SPARK_IMAGE=spark:3.5.7-scala2.12-java17-ubuntu
```

Se usa una tag real de la Docker Official Image de Spark `3.5.x` con `Java 17`.

## Arranque

Desde la raíz del repo:

```bash
docker compose -f spark_baseline/docker-compose.yml up -d
```

La UI del master queda en `http://localhost:8080`.

## Compilación del job empaquetado

Esta parte es opcional para la primera iteración. El baseline ya puede ejecutar
SSSP sin `.jar`, mediante `spark-shell`.

Si luego quieres pasar a jobs empaquetados, `build-job.sh` requiere `Java 17`
en host. Si no tienes `sbt`, el script intentará usar una imagen Docker de
`sbt` si defines `SBT_IMAGE`.

```bash
./spark_baseline/scripts/build-job.sh
```

Ejemplo con compilación en Docker:

```bash
export SBT_IMAGE=sbtscala/scala-sbt:eclipse-temurin-17.0.18_1.10.1_2.12.18
./spark_baseline/scripts/build-job.sh
```

El `.jar` generado queda en:

```text
spark_baseline/jobs/target/scala-2.12/spark-baseline_2.12-0.1.0.jar
```

## Dataset de entrada

El job de ejemplo espera un fichero TSV con:

- `src<TAB>dst<TAB>weight`
- o `src<TAB>dst` y entonces asume peso `1.0`

Para reutilizar tus datasets actuales, copia o enlaza el fichero al directorio:

```text
spark_baseline/data/
```

## Ejecución

Ejemplo:

```bash
./spark_baseline/scripts/submit-sssp.sh \
  /opt/tfm-spark/data/large_sssp_100000.txt \
  /opt/tfm-spark/results/sssp_100000 \
  0 \
  4
```

Argumentos:

1. ruta del grafo dentro del contenedor
2. ruta de salida dentro del contenedor
3. nodo fuente
4. número de particiones

En la versión actual del runner, la salida completa del vector de distancias no se
persiste por defecto: el objetivo inicial es obtener métricas de benchmark
comparables. La ruta de salida se mantiene para compatibilidad con una versión
posterior que sí persista resultados cuando haga falta.

## Métricas

El runner actual imprime un JSON con:

- `load_time_ms`
- `execution_time_ms`
- `total_time_ms`
- `reachable_nodes`
- `max_distance`

Esto permite compararlo con la estructura que ya usas en `standalone` y `burst`.

## Campaña agregada

Para lanzar las baselines Spark de los algoritmos de grafo del TFM:

```bash
python spark_baseline/run_spark_graph_benchmarks.py
```

Por defecto cubre:

- `bfs`
- `sssp`
- `labelpropagation`
- `louvain`

En `louvain`, el runner deja constancia explícita de que no hay todavía una
implementación Spark semánticamente equivalente y por tanto la baseline se marca
como no comparable en vez de forzar una comparación injusta.
