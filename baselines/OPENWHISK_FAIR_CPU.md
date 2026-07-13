# OpenWhisk Fair CPU

Para acercar la comparación `Burst` vs `Spark` a un presupuesto de CPU por
worker más justo, este experimento asume:

- `Spark`: `4` workers, `1 vCPU` real por worker, `1` core por executor
- `Burst`: `4` workers lógicos, `granularity=1`, y `1 vCPU` por pod de acción

El fichero [openwhisk_action_cpu_limitrange.yaml](/home/sergio/src/labelpropagation/spark_baseline/openwhisk_action_cpu_limitrange.yaml)
define un `LimitRange` que fuerza `requests.cpu=1` y `limits.cpu=1` por
contenedor para los pods del namespace `openwhisk` que no declaren CPU
explícitamente.

Aplicación:

```bash
kubectl apply -f /home/sergio/src/labelpropagation/spark_baseline/openwhisk_action_cpu_limitrange.yaml
```

Verificación sugerida:

```bash
kubectl -n openwhisk get limitrange
kubectl -n openwhisk describe limitrange openwhisk-action-default-cpu
```

Nota metodológica:

- Este `LimitRange` se aplicará a contenedores del namespace `openwhisk` que no
  declaren CPU explícitamente.
- Si el despliegue de control plane ya fija sus propios `requests/limits`, esos
  valores prevalecen.
- Si algún componente del plano de control no fija CPU, también heredará este
  default al recrearse.
- Por eso conviene aplicarlo y luego verificar un pod de acción real para
  confirmar que la política está entrando justo donde queremos.
