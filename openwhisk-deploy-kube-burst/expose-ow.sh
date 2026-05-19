#!/bin/bash
echo "🚀 Iniciando redirección de puertos para todos los servicios..."

# OpenWhisk
kubectl port-forward -n openwhisk svc/owdev-nginx 31001:443 --address 0.0.0.0 &

# RabbitMQ
kubectl port-forward svc/rabbitmq 5672:5672 --address 0.0.0.0 &
kubectl port-forward svc/rabbitmq 15672:15672 --address 0.0.0.0 &

# Redis (Dragonfly)
kubectl port-forward svc/dragonfly 6379:6379 --address 0.0.0.0 &

# MinIO
kubectl port-forward svc/minio-service 9000:9000 --address 0.0.0.0 &
kubectl port-forward svc/minio-service 9001:9001 --address 0.0.0.0 &

echo "✅ Todos los puertos están siendo redireccionados a 0.0.0.0"
echo "OpenWhisk: 31001"
echo "RabbitMQ: 5672, 15672"
echo "Redis: 6379"
echo "MinIO: 9000, 9001"

wait
