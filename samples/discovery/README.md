# Distributed Daemonize Discovery

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

```

### 3) query discovery infomation via http request
```
# query all jobs 
curl http://127.0.0.1:12344/api/v1/discovery/getalljobs

["job1","job2","job3"]

# get member information by job id
curl http://127.0.0.1:12344/api/v1/discovery/getbyjob/job1

{"cluster":"cluster2","id":"2eeaf95a-aff7-4dcd-afc2-f1f867d30989","name":"dd3","url":"http://127.0.0.1:12344"}
```
