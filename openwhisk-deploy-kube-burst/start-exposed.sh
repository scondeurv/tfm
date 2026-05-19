#!/bin/bash
set -euo pipefail

echo "🚀 Arrancando Minikube con mapeo de puertos..."
WORKER_COUNT=${OW_WORKER_COUNT:-4}
CPU_PER_WORKER=${OW_CPU_PER_WORKER:-1}
SYSTEM_RESERVED_CPUS=${OW_SYSTEM_RESERVED_CPUS:-6}
MEMORY_PER_WORKER_MB=${OW_MEMORY_PER_WORKER_MB:-4096}
SYSTEM_RESERVED_MEM_MB=${OW_SYSTEM_RESERVED_MEM_MB:-8192}
HOST_RESERVED_CPUS=${OW_HOST_RESERVED_CPUS:-2}
HOST_RESERVED_MEM_MB=${OW_HOST_RESERVED_MEM_MB:-8192}
CLUSTER_CPUS_OVERRIDE=${OW_CLUSTER_CPUS:-}
CLUSTER_MEMORY_OVERRIDE_MB=${OW_CLUSTER_MEMORY_MB:-}

TOTAL_CPUS=$(nproc)
TOTAL_MEM=$(free -m | awk '/^Mem:/{print $2}')

TARGET_CPUS=$((WORKER_COUNT * CPU_PER_WORKER + SYSTEM_RESERVED_CPUS))
TARGET_MEM=$((WORKER_COUNT * MEMORY_PER_WORKER_MB + SYSTEM_RESERVED_MEM_MB))

MAX_CPUS=$((TOTAL_CPUS - HOST_RESERVED_CPUS))
if [ "$MAX_CPUS" -lt 2 ]; then MAX_CPUS=2; fi
MAX_MEM=$((TOTAL_MEM - HOST_RESERVED_MEM_MB))
if [ "$MAX_MEM" -lt 2048 ]; then MAX_MEM=2048; fi

CPUS=${CLUSTER_CPUS_OVERRIDE:-$TARGET_CPUS}
MEM=${CLUSTER_MEMORY_OVERRIDE_MB:-$TARGET_MEM}

if [ "$CPUS" -gt "$MAX_CPUS" ]; then CPUS=$MAX_CPUS; fi
if [ "$MEM" -gt "$MAX_MEM" ]; then MEM=$MAX_MEM; fi

if [ "$CPUS" -lt 2 ]; then CPUS=2; fi
if [ "$MEM" -lt 2048 ]; then MEM=2048; fi

echo "⚙️  Politica de workers: ${WORKER_COUNT} workers x ${CPU_PER_WORKER} CPU = $((WORKER_COUNT * CPU_PER_WORKER)) CPUs de usuario"
echo "⚙️  Reserva de sistema: ${SYSTEM_RESERVED_CPUS} CPUs, ${SYSTEM_RESERVED_MEM_MB}MB RAM"
echo "⚙️  Reserva del host: ${HOST_RESERVED_CPUS} CPUs, ${HOST_RESERVED_MEM_MB}MB RAM"
if [ -n "${CLUSTER_CPUS_OVERRIDE}" ] || [ -n "${CLUSTER_MEMORY_OVERRIDE_MB}" ]; then
  echo "⚙️  Override explicito del cluster: CPUs=${CLUSTER_CPUS_OVERRIDE:-auto}, RAM=${CLUSTER_MEMORY_OVERRIDE_MB:-auto}MB"
fi
echo "⚙️  Configurando Minikube con límites: CPUs=$CPUS, RAM=${MEM}MB"

minikube start --driver docker --cpus $CPUS --memory ${MEM}m --ports 31001:31001,5672:30672,15672:31672,6379:31379,9000:30000,9001:30001

echo "⏳ Esperando a que Minikube quede operativo..."
minikube status
kubectl get nodes >/dev/null

echo "📥 Precargando imágenes en minikube para evitar ImagePullBackOff..."
REQUIRED_IMAGES=(
  "busybox:latest"
  "zookeeper:3.4"
  "rabbitmq:3-management"
  "redis:4.0"
  "wurstmeister/kafka:2.12-2.3.1"
  "manriurv/controller:classic"
  "manriurv/invoker:classic"
  "nginx:1.21.1"
  "openwhisk/alarmprovider:2.3.0"
  "openwhisk/kafkaprovider:2.1.0"
)
for img in "${REQUIRED_IMAGES[@]}"; do
  echo "  loading $img..."
  minikube image load "$img"
done
echo "✅ Imágenes precargadas."

echo "🏷️  Etiquetando el nodo para que acepte Invokers..."
kubectl label nodes --all openwhisk-role=invoker --overwrite

echo "📦 Desplegando servicios auxiliares (RabbitMQ, MinIO, Redis/Dragonfly)..."

echo "⏳ Esperando a que el sistema esté listo..."
until kubectl get serviceaccount default > /dev/null 2>&1; do sleep 2; done

kubectl apply -f infrastructure.yaml

echo "🏗️ Creando namespace 'openwhisk'..."
kubectl create namespace openwhisk --dry-run=client -o yaml | kubectl apply -f -

echo "🔄 Instalando/Actualizando OpenWhisk..."
helm upgrade --install owdev ./helm/openwhisk -n openwhisk -f mycluster.yaml

echo "✅ OpenWhisk debería estar disponible en: https://localhost:31001"
echo "Recuerda que si el clúster es nuevo, los pods pueden tardar un poco en estar 'Running'."
