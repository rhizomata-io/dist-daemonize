# Distributed Daemonize Sample

### 1) run etcd first
```
REGISTRY=quay.io/coreos/etcd
# available from v3.2.5
REGISTRY=gcr.io/etcd-development/etcd
NODE1=127.0.0.1

docker run \
  -p 2379:2379 \
  -p 2380:2380 \
  -d \
  --name etcd ${REGISTRY}:latest \
  /usr/local/bin/etcd \
  --data-dir=/etcd-data --name node1 \
  --initial-advertise-peer-urls http://${NODE1}:2380 --listen-peer-urls http://0.0.0.0:2380 \
  --advertise-client-urls http://${NODE1}:2379 --listen-client-urls http://0.0.0.0:2379 \
  --initial-cluster node1=http://${NODE1}:2380
```

### 2) run 4 dist-demonize processes on different ports
```
go run . -cluster cluster1 -name dd1 -exposed-host 127.0.0.1 -port 12345 -etcd-urls http://127.0.0.1:2379

go run . -cluster cluster1 -name dd2 -exposed-host 127.0.0.1 -port 12346 -etcd-urls http://127.0.0.1:2379

go run . -cluster cluster1 -name dd3 -exposed-host 127.0.0.1 -port 12347 -etcd-urls http://127.0.0.1:2379

go run . -cluster cluster1 -name dd4 -exposed-host 127.0.0.1 -port 12348 -etcd-urls http://127.0.0.1:2379
```

### 3) kill dist-demonize processes and re-run killed processes
```
 

```
