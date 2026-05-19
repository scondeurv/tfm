# Deployment in k8s for BurstWhisk

This repo contains little modifications from the original Openwhisk deployment in Kubernetes. The main modifications are:

 - Accessing to custom Controller and Invoker artifacts.
 - Adjusting configuration limits and bounds (runtime memory, concurrent executions, upping blackbox fraction to 100%...) for a real deployment in cluster.


### Deploy steps

1. Configure and start your k8s cluster.
2. Label the nodes. In this case, we will label the nodes with `openwhisk-role=invoker` and `openwhisk-role=core`.
3. Clone this repo, access it and over base folder, execute:
    ```bash 
   helm install owdev ./helm/openwhisk -n openwhisk --create-namespace -f mycluster.yaml
    ```
   over control-plane node in k8s cluster.

> Maybe modifications in `mycluster.yaml` are necessary to adapt to your cluster.
