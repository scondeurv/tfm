Use this path for a temporary CloudLab smoke test without mutating shared cluster-wide configuration.

Default release:
- namespace: `ow-sconde-smoke`
- helm release: `ow-sconde-smoke`
- API access: `kubectl port-forward` to `http://127.0.0.1:31001`
- burst middleware for the LP smoke test: `redis-list`
- note: the Helm values still provide a RabbitMQ URI, but the recommended LP smoke-test path does not use RabbitMQ
- note: CloudLab values explicitly allow `user-action-pod -> <release>-redis:6379` so Burst middleware can use Redis without opening backend ingress more broadly

Flow:
1. `bash openwhisk-deploy-kube-burst/cloudlab/deploy-smoke-openwhisk.sh`
2. `bash openwhisk-deploy-kube-burst/cloudlab/port-forward-smoke-openwhisk.sh`
3. export the CloudLab MinIO credentials in `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`
4. run `bash labelpropagation/run_cloudlab_smoke_lp.sh`
5. `bash openwhisk-deploy-kube-burst/cloudlab/cleanup-smoke-openwhisk.sh`
