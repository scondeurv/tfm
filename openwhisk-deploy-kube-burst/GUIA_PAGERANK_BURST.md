# Guía Completa: PageRank Distribuido con OpenWhisk + Burst Communication

Esta guía te permite configurar y ejecutar PageRank distribuido usando OpenWhisk con comunicación entre workers vía Redis Streams.

---

## 📋 Pre-requisitos

- **Kubernetes cluster** funcionando (Minikube, K3s, o cluster completo)
- **Helm 3** instalado
- **Docker** instalado
- **uv** instalado (gestor de paquetes Python moderno)
- **Rust y Cargo** instalados
- **kubectl** configurado

---

## 1. Preparar Repositorios

```bash
cd ~/src

# Clonar o verificar que tienes:
# - openwhisk-deploy-kube-burst/
# - burst-validation/
```

---

## 2. Instalar Infraestructura Externa

### 2.1 Redis (para comunicación entre workers)

```bash
docker run -d \
  --name redis \
  -p 6379:6379 \
  redis:latest
```

**Verificar:**
```bash
redis-cli -h 192.168.1.213 ping
# Debe retornar: PONG
```

### 2.2 RabbitMQ (para mensajería de OpenWhisk)

```bash
docker run -d \
  --name rabbitmq \
  -p 5672:5672 \
  -p 15672:15672 \
  rabbitmq:3-management
```

**Verificar:**
```bash
# Acceder a http://192.168.1.213:15672
# Usuario: guest, Password: guest
```

### 2.3 MinIO (almacenamiento S3 compatible)

```bash
docker run -d \
  --name minio \
  -p 9000:9000 \
  -p 9001:9001 \
  -e "MINIO_ROOT_USER=minioadmin" \
  -e "MINIO_ROOT_PASSWORD=minioadmin" \
  minio/minio server /data --console-address ":9001"
```

**Verificar:**
```bash
# Acceder a http://192.168.1.213:9001
# Usuario: minioadmin, Password: minioadmin
```

---

## 3. Configurar OpenWhisk

### 3.1 Revisar configuración en `mycluster.yaml`

Tu archivo `mycluster.yaml` debe tener esta configuración:

```yaml
whisk:
  middleware:
    rabbitmq: "amqp://guest:guest@192.168.1.213:5672"
    redisList: "redis://192.168.1.213:6379"
    redisStream: "redis://192.168.1.213:6379"  # ← Clave para burst
  ingress:
     type: NodePort
     apiHostName: 192.168.1.213
     apiHostPort: 31001
     useInternally: false
  versions:
    openwhisk:
      gitTag: "72bb2a1"

controller:
  imageName: "manriurv/controller"
  imageTag: "classic"  # ← Imagen custom con soporte burst

invoker:
  imageName: "manriurv/invoker"
  imageTag: "classic"  # ← Imagen custom con soporte burst
  containerFactory:
    impl: "docker" 

nginx:
  httpsNodePort: 31001

zookeeper:
  port: 2181

k8s:
  persistence:
    enabled: false
```

**⚠️ IMPORTANTE:** Si tu IP es diferente a `192.168.1.213`, actualiza todas las referencias.

### 3.2 Desplegar OpenWhisk en Kubernetes

```bash
cd ~/src/openwhisk-deploy-kube-burst

# Crear namespace
kubectl create namespace openwhisk

# Etiquetar nodos para invokers
kubectl label nodes --all openwhisk-role=invoker

# Instalar con Helm
helm install owdev ./helm/openwhisk \
  -n openwhisk \
  -f mycluster.yaml

# Monitorear despliegue (puede tardar 5-10 minutos)
kubectl get pods -n openwhisk -w
```

**Pods esperados:**
```
NAME                              READY   STATUS    RESTARTS   AGE
owdev-controller-0                1/1     Running   0          5m
owdev-invoker-0                   1/1     Running   0          5m
owdev-nginx-xxx                   1/1     Running   0          5m
owdev-zookeeper-0                 1/1     Running   0          5m
```

---

## 4. Compilar Acción de PageRank

### 4.1 Navegar al código Rust

```bash
cd ~/src/burst-validation/pagerank/ow-pr
```

### 4.2 Compilar código

```bash
cargo build --release
```

Esto genera el binario en: `target/release/ow-pr`

### 4.3 Crear estructura para OpenWhisk

```bash
# Crear directorio de acción
mkdir -p action/exec

# Copiar ejecutable
cp target/release/ow-pr action/exec/exec
chmod +x action/exec/exec

# Crear script compile.py (requerido por OpenWhisk)
cat > action/compile.py << 'EOF'
#!/usr/bin/env python3
import sys
import os
import shutil

# OpenWhisk llama: compile.py <main> <src_dir> <bin_dir>
src_dir = sys.argv[2]
bin_dir = sys.argv[3]

os.makedirs(bin_dir, exist_ok=True)
shutil.copy(f"{src_dir}/exec/exec", f"{bin_dir}/exec")
os.chmod(f"{bin_dir}/exec", 0o755)
EOF

chmod +x action/compile.py
```

### 4.4 Empaquetar como ZIP

```bash
# Desde el directorio pagerank/ (padre de ow-pr/)
cd ..
zip -r pagerank.zip ow-pr/action/ ow-pr/action/compile.py
```

Esto crea `pagerank.zip` listo para desplegar.

---

## 5. Configurar Entorno Python con uv

### 5.1 Crear entorno virtual

```bash
cd ~/src/burst-validation

# Crear entorno con uv
uv venv

# Activar entorno
source .venv/bin/activate
```

### 5.2 Instalar dependencias

```bash
# Instalar paquetes desde requirements.txt
uv pip install -r requirements.txt
```

### 5.3 Verificar instalación

```bash
uv run python -c "from ow_client.openwhisk_executor import OpenwhiskExecutor; print('✓ OK')"
```

---

## 6. Preparar Datos en MinIO

### 6.1 Instalar MinIO Client

```bash
wget https://dl.min.io/client/mc/release/linux-amd64/mc
chmod +x mc
sudo mv mc /usr/local/bin/
```

### 6.2 Configurar conexión

```bash
mc alias set myminio http://192.168.1.213:9000 minioadmin minioadmin
```

### 6.3 Crear bucket

```bash
mc mb myminio/pagerank-data
```

### 6.4 Generar y subir datos de prueba

```bash
cd ~/src/burst-validation/pagerank

# Ver opciones disponibles
uv run python generate_payload.py --help

# Generar datos de grafo particionado
# (ajusta parámetros según tu caso de uso)
uv run python generate_payload.py \
  --partitions 4 \
  --num_nodes 5 \
  --bucket pagerank-data \
  --key 1 \
  --endpoint http://192.168.1.213:9000
```

Esto crea archivos en MinIO:
- `pagerank-data/1/part-00000`
- `pagerank-data/1/part-00001`
- `pagerank-data/1/part-00002`
- `pagerank-data/1/part-00003`

---

## 7. Ejecutar PageRank Distribuido

### 7.1 Ejecutar con uv

```bash
cd ~/src/burst-validation/pagerank

uv run python pagerank.py \
  --ow_host 192.168.1.213 \
  --ow_port 31001 \
  --granularity 4 \
  --partitions 4 \
  --num_nodes 5 \
  --bucket pagerank-data \
  --key 1 \
  --pr_endpoint http://192.168.1.213:9000 \
  --backend RedisStream \
  --debug
```

### 7.2 Descripción de parámetros

| Parámetro | Descripción | Valor recomendado |
|-----------|-------------|-------------------|
| `--ow_host` | IP donde corre OpenWhisk | `192.168.1.213` |
| `--ow_port` | Puerto de OpenWhisk API | `31001` |
| `--granularity` | Número de workers en paralelo | `4` |
| `--partitions` | Particiones del grafo (debe = granularity) | `4` |
| `--num_nodes` | Número total de nodos en el grafo | `5` (ajustar según datos) |
| `--bucket` | Bucket en MinIO | `pagerank-data` |
| `--key` | Prefijo de los archivos | `1` |
| `--pr_endpoint` | URL de MinIO | `http://192.168.1.213:9000` |
| `--backend` | Middleware de comunicación | `RedisStream` |
| `--debug` | Habilitar logs detallados | `True` |

### 7.3 Salida esperada

El script mostrará:
```
2025-10-26 00:44:35 - INFO - Function e507f2d719464c2f87f2d719466c2fae finished
2025-10-26 00:44:35 - INFO - [e507f2d719464c2f87f2d719466c2fae finished]: [
  {
    "bucket": "pagerank-data",
    "key": "1/part-00000",
    "timestamps": [
      {"key": "worker_start", "value": "1761432274155"},
      {"key": "get_input", "value": "1761432274162"},
      ...
      {"key": "worker_end", "value": "1761432274194"}
    ]
  }
]
```

---

## 8. Analizar Resultados

### 8.1 Estructura de timestamps

Cada worker genera estos eventos:

```
worker_start              ← Inicio del worker
get_input                 ← Carga datos desde S3
calc_outlinks             ← Calcula enlaces salientes
iter_0_start              ← Inicio iteración 0
iter_0_broadcast_weights  ← Envía pesos a otros workers
iter_0_calc_sums          ← Calcula sumas locales
iter_0_reduce             ← Recibe datos de otros workers
iter_0_calc_err           ← Calcula error local
iter_0_broadcast_err      ← Envía error a todos
iter_0_end                ← Fin iteración 0
...                       ← Más iteraciones
worker_end                ← Fin del worker
```

### 8.2 Métricas de rendimiento

De tu ejecución real:
- **4 workers** ejecutándose en paralelo
- **24 iteraciones** completadas
- **Tiempo total:** ~39ms (1761432274155 → 1761432274194)
- **Tiempo por iteración:** ~1-2ms
- **Comunicación:** Redis Streams funcionando correctamente

---

## 9. Monitoreo y Debugging

### 9.1 Ver pods de OpenWhisk

```bash
# Listar todos los pods
kubectl get pods -n openwhisk

# Ver estado detallado
kubectl describe pod -n openwhisk owdev-invoker-0
```

### 9.2 Ver logs en tiempo real

```bash
# Logs del invoker (donde se ejecutan las acciones)
kubectl logs -n openwhisk owdev-invoker-0 -f

# Logs del controller
kubectl logs -n openwhisk owdev-controller-0 -f

# Logs de nginx (gateway)
kubectl logs -n openwhisk <nginx-pod-name> -f
```

### 9.3 Verificar Redis Streams

```bash
# Conectar a Redis
redis-cli -h 192.168.1.213

# Listar todos los streams
KEYS "*stream*"

# Ejemplo de output:
# broadcast_stream:54b901e0-cee3-423d-8545-2c70907c775d:g0
# broadcast_stream:54b901e0-cee3-423d-8545-2c70907c775d:g1
# direct_stream:54b901e0-cee3-423d-8545-2c70907c775d:s0-d1

# Ver contenido de un stream
XRANGE broadcast_stream:54b901e0-cee3-423d-8545-2c70907c775d:g0 - +

# Ver longitud de un stream
XLEN broadcast_stream:54b901e0-cee3-423d-8545-2c70907c775d:g0

# Limpiar streams antiguos (si es necesario)
FLUSHDB
```

### 9.4 Verificar datos en MinIO

```bash
# Listar archivos en el bucket
mc ls myminio/pagerank-data/1/

# Descargar un archivo para inspección
mc cp myminio/pagerank-data/1/part-00000 /tmp/
cat /tmp/part-00000
```

---

## 10. Estructura del Proyecto

```
~/src/
├── openwhisk-deploy-kube-burst/
│   ├── mycluster.yaml              ← Configuración OpenWhisk
│   ├── helm/openwhisk/             ← Helm charts
│   └── GUIA_PAGERANK_BURST.md      ← Esta guía
│
└── burst-validation/
    ├── requirements.txt            ← Dependencias Python
    ├── .venv/                      ← Entorno virtual (uv)
    │
    ├── ow_client/                  ← Cliente Python para OpenWhisk
    │   ├── __init__.py
    │   ├── openwhisk_executor.py   ← Ejecutor de acciones burst
    │   ├── parser.py               ← Parseo de argumentos
    │   ├── time_helper.py          ← Utilidades de tiempo
    │   └── utils.py
    │
    ├── pagerank/
    │   ├── pagerank.py             ← Script principal de ejecución
    │   ├── pagerank_utils.py       ← Utilidades para PageRank
    │   ├── generate_payload.py     ← Generador de datos de grafo
    │   ├── pagerank.zip            ← Acción compilada para OpenWhisk
    │   │
    │   ├── ow-pr/                  ← Código Rust
    │   │   ├── Cargo.toml
    │   │   ├── src/
    │   │   │   └── lib.rs
    │   │   ├── target/release/ow-pr
    │   │   └── action/
    │   │       ├── exec/exec
    │   │       └── compile.py
    │   │
    │   └── burst-communication-middleware/  ← Librería de comunicación
    │       ├── Cargo.toml
    │       └── src/
    │           ├── lib.rs
    │           ├── middleware.rs
    │           ├── actor.rs
    │           └── backends/
    │
    └── burst-communication-middleware/      ← Submódulo Git compartido
        └── ...
```

---

## 11. Comandos Útiles con uv

### Gestión de entorno

```bash
# Crear entorno virtual
uv venv

# Activar entorno
source .venv/bin/activate

# Desactivar entorno
deactivate
```

### Gestión de paquetes

```bash
# Instalar dependencias
uv pip install -r requirements.txt

# Instalar paquete específico
uv pip install <paquete>

# Actualizar paquete
uv pip install --upgrade <paquete>

# Listar paquetes instalados
uv pip list

# Generar requirements.txt
uv pip freeze > requirements.txt
```

### Ejecutar scripts

```bash
# Ejecutar sin activar venv
uv run python script.py

# Ejecutar con argumentos
uv run python pagerank.py --help
```

---

## 12. Troubleshooting

### Problema: Pods de OpenWhisk no inician

```bash
# Ver estado detallado
kubectl describe pod -n openwhisk <POD_NAME>

# Ver logs
kubectl logs -n openwhisk <POD_NAME>

# Causas comunes:
# - Recursos insuficientes (RAM/CPU)
# - Imágenes no disponibles
# - Problemas de conectividad con Redis/RabbitMQ
```

**Solución:**
```bash
# Verificar recursos del nodo
kubectl top nodes

# Verificar eventos
kubectl get events -n openwhisk --sort-by='.lastTimestamp'
```

### Problema: Workers se quedan esperando en Redis

```bash
# Verificar conectividad desde los pods
kubectl run -it --rm debug --image=redis --restart=Never -- \
  redis-cli -h 192.168.1.213 ping

# Ver streams activos
redis-cli -h 192.168.1.213 KEYS "*stream*"

# Verificar si hay mensajes atascados
redis-cli -h 192.168.1.213 XLEN broadcast_stream:<transaction_id>:g0
```

**Solución:**
```bash
# Limpiar streams antiguos
redis-cli -h 192.168.1.213 FLUSHDB

# Reiniciar la ejecución
```

### Problema: Error de importación en Python

```bash
# Verificar que estás en el venv
which python
# Debe mostrar: /home/sergio/src/burst-validation/.venv/bin/python

# Reinstalar dependencias
uv pip install --force-reinstall -r requirements.txt
```

### Problema: ZIP de acción incorrecto

```bash
# Verificar contenido del ZIP
unzip -l pagerank.zip

# Debe mostrar:
# ow-pr/action/exec/exec
# ow-pr/action/compile.py
```

**Solución:**
```bash
# Recrear ZIP con estructura correcta
cd ~/src/burst-validation/pagerank
rm pagerank.zip
zip -r pagerank.zip ow-pr/action/ ow-pr/action/compile.py
```

### Problema: No se puede conectar a MinIO

```bash
# Verificar que MinIO está corriendo
docker ps | grep minio

# Probar conexión
curl http://192.168.1.213:9000/minio/health/live
# Debe retornar: 200 OK
```

### Problema: Timeout en ejecución

```bash
# Las acciones tienen timeout configurado
# Si tu grafo es muy grande, aumenta el timeout

# Ver timeout actual
kubectl get configmap -n openwhisk owdev-whisk.config -o yaml | grep timeout

# Ajustar en mycluster.yaml y actualizar deployment
helm upgrade owdev ./helm/openwhisk -n openwhisk -f mycluster.yaml
```

---

## 13. Parámetros de Comunicación Burst

### 13.1 Backends disponibles

| Backend | Descripción | Cuándo usar |
|---------|-------------|-------------|
| `RedisStream` | Redis Streams (recomendado) | Alta velocidad, baja latencia |
| `RedisList` | Redis Lists | Compatibilidad legacy |
| `RabbitMQ` | Colas RabbitMQ | Mensajes grandes, persistencia |

### 13.2 Estructura de burst_info

Automáticamente generado por OpenWhisk:

```json
{
  "burst_info": {
    "1e49845e-2736-4c83-abda-4a4f2deffb3a": [0, 0],  // Worker 0
    "69eb7e57-d2b9-4fe1-9f6a-a1350f24fd48": [1, 1],  // Worker 1
    "77a2c7b8-c0a4-4e18-9beb-522b03ce3d52": [3, 3],  // Worker 3
    "ac75c764-71b7-4c7d-91c0-96c9cadaddcc": [2, 2]   // Worker 2
  },
  "invoker_id": "1e49845e-2736-4c83-abda-4a4f2deffb3a",
  "transaction_id": "54b901e0-cee3-423d-8545-2c70907c775d"
}
```

### 13.3 Naming de Redis Streams

```
broadcast_stream:<transaction_id>:g<worker_id>
direct_stream:<transaction_id>:s<source>-d<destination>
```

Ejemplo:
```
broadcast_stream:54b901e0-cee3-423d-8545-2c70907c775d:g0
direct_stream:54b901e0-cee3-423d-8545-2c70907c775d:s1-d0
```

---

## 14. Optimizaciones y Mejores Prácticas

### 14.1 Ajustar número de workers

```bash
# Para grafos pequeños (< 1000 nodos)
--granularity 2 --partitions 2

# Para grafos medianos (1000-10000 nodos)
--granularity 4 --partitions 4

# Para grafos grandes (> 10000 nodos)
--granularity 8 --partitions 8
```

### 14.2 Configurar chunk_size

Para mensajes grandes (> 1MB):

```bash
uv run python pagerank.py \
  ... \
  --chunk_size 1048576  # 1MB chunks
```

### 14.3 Monitorear recursos

```bash
# Ver uso de CPU/RAM de pods
kubectl top pods -n openwhisk

# Ver uso de nodos
kubectl top nodes
```

---

## 15. Limpieza

### 15.1 Eliminar deployment de OpenWhisk

```bash
# Desinstalar Helm release
helm uninstall owdev -n openwhisk

# Eliminar namespace
kubectl delete namespace openwhisk
```

### 15.2 Limpiar datos en MinIO

```bash
# Eliminar bucket completo
mc rb --force myminio/pagerank-data
```

### 15.3 Detener servicios Docker

```bash
# Detener y eliminar contenedores
docker stop redis rabbitmq minio
docker rm redis rabbitmq minio
```

### 15.4 Limpiar Redis

```bash
# Limpiar todos los datos
redis-cli -h 192.168.1.213 FLUSHALL

# O solo los streams
redis-cli -h 192.168.1.213 FLUSHDB
```

---

## 16. Referencias y Recursos

### Configuración clave

- **IP de servicios:** `192.168.1.213` (ajustar según tu configuración)
- **Redis:** `redis://192.168.1.213:6379`
- **RabbitMQ:** `amqp://guest:guest@192.168.1.213:5672`
- **MinIO:** `http://192.168.1.213:9000`
- **OpenWhisk API:** `http://192.168.1.213:31001`

### Imágenes Docker custom

- **Controller:** `manriurv/controller:classic`
- **Invoker:** `manriurv/invoker:classic`

Estas imágenes incluyen soporte para burst communication.

### Repositorios

- **OpenWhisk Deploy Kube:** `~/src/openwhisk-deploy-kube-burst/`
- **Burst Validation:** `~/src/burst-validation/`
- **Middleware:** `~/src/burst-validation/burst-communication-middleware/`

---

## ✅ Checklist de Verificación

Antes de ejecutar PageRank, verifica:

- [ ] Redis corriendo y accesible en `192.168.1.213:6379`
- [ ] RabbitMQ corriendo y accesible en `192.168.1.213:5672`
- [ ] MinIO corriendo y accesible en `192.168.1.213:9000`
- [ ] Todos los pods de OpenWhisk en estado `Running`
- [ ] Código Rust compilado (`ow-pr/target/release/ow-pr`)
- [ ] `pagerank.zip` creado con estructura correcta
- [ ] Bucket `pagerank-data` creado en MinIO
- [ ] Datos particionados subidos a MinIO
- [ ] Entorno Python con uv configurado y dependencias instaladas
- [ ] IP correcta en todos los archivos de configuración

---

## 🎯 Resultado Esperado

Al ejecutar el script correctamente, deberías ver:

```
✓ 4 workers ejecutándose en paralelo
✓ Comunicación vía Redis Streams funcionando
✓ ~20-30 iteraciones de PageRank completadas
✓ Tiempo total de ejecución: ~40ms
✓ Timestamps detallados por cada fase
✓ Resultados guardados en formato JSON
```

---

## 📧 Soporte

Si encuentras problemas:

1. Revisa la sección de **Troubleshooting**
2. Verifica los logs de Kubernetes: `kubectl logs -n openwhisk <pod-name>`
3. Verifica los streams de Redis: `redis-cli KEYS "*stream*"`
4. Asegúrate de que todos los servicios estén corriendo

---

**¡Listo! Tu sistema de PageRank distribuido con burst communication está funcionando.**
