# DA3408 Assignment 3: Spark vs. Ray

The same NYC Yellow Taxi cleaning pipeline in Apache Spark (`spark_clean.py`) and Ray Data (`ray_clean.py`), each run on a cluster with two workers. `compare_outputs.py` checks that both produce the same result.

Nodes: `node0` (Spark master / Ray head), `node1` and `node2` (workers). The folder `~/a3` on the host is `/data` on every node.

## 1. Network and nodes (host)

```bash
mkdir -p ~/a3/raw
cp spark_clean.py ray_clean.py compare_outputs.py ~/a3/
docker network create a3net
docker run -dit --name node0 --hostname node0 --network a3net -p 8080:8080 -p 8265:8265 -v ~/a3:/data ubuntu:24.04 bash
docker run -dit --name node1 --hostname node1 --network a3net --cpus 2 --memory 6g --shm-size 2g -v ~/a3:/data ubuntu:24.04 bash
docker run -dit --name node2 --hostname node2 --network a3net --cpus 2 --memory 6g --shm-size 2g -v ~/a3:/data ubuntu:24.04 bash
```

Open a shell on each node, one terminal each:

```bash
docker exec -it node0 bash
docker exec -it node1 bash
docker exec -it node2 bash
```

## 2. Install (all nodes)

```bash
apt update && apt install -y openjdk-17-jre-headless python3-pip curl procps
pip install --break-system-packages pyspark==3.5.8 "ray[data,default]==2.59.0" pandas==2.2.3 pyarrow==18.1.0
```

## 3. Data (node0)

```bash
cd /data/raw
for m in 2024-01 2024-02 2024-03; do curl -fLO https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_$m.parquet; done
curl -fLO https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv
```

## 4. Spark

```bash
# node0
nohup spark-class org.apache.spark.deploy.master.Master --host node0 > /tmp/master.log 2>&1 &

# node1 (use --host node2 on node2)
nohup spark-class org.apache.spark.deploy.worker.Worker spark://node0:7077 --host node1 --cores 2 --memory 4g > /tmp/worker.log 2>&1 &
```

Spark UI: http://localhost:8080

```bash
# host: record CPU and memory while the job runs (Ctrl+C when done)
while true; do docker stats --no-stream --format "{{.Name}},{{.CPUPerc}},{{.MemUsage}}" node1 node2 >> ~/a3/spark_stats.csv; sleep 1; done

# node0: run
cd /data
spark-submit --master spark://node0:7077 --executor-memory 3g spark_clean.py --input /data/raw --zones /data/raw/taxi_zone_lookup.csv --output /data/out/spark

# all nodes: stop
pkill -f spark
```

## 5. Ray

```bash
# node0
ray start --head --num-cpus=0 --memory=0 --dashboard-host=0.0.0.0

# node1 and node2
ray start --address=node0:6379 --num-cpus=2
```

Ray dashboard: http://localhost:8265 (Cluster tab)

```bash
# host: record CPU and memory while the job runs (Ctrl+C when done)
while true; do docker stats --no-stream --format "{{.Name}},{{.CPUPerc}},{{.MemUsage}}" node1 node2 >> ~/a3/ray_stats.csv; sleep 1; done

# node0: run
cd /data
python3 ray_clean.py --input /data/raw --zones /data/raw/taxi_zone_lookup.csv --output /data/out/ray --max-aggregators 1

# all nodes: stop
ray stop
```

## 6. Results

```bash
# node0: check both outputs match, show timings
cd /data
python3 compare_outputs.py /data/out/spark /data/out/ray
cat /data/results/runs.csv
```

```bash
# host: peak CPU and memory per worker
cd ~/a3
for f in spark_stats.csv ray_stats.csv; do echo "== $f"; for n in node1 node2; do awk -F, -v n=$n '$1==n {c=$2; sub(/%/,"",c); split($3,a," "); m=a[1]; if (m ~ /GiB/) {sub(/GiB/,"",m); m*=1024} else {sub(/MiB/,"",m)} if (c+0>pc) pc=c+0; if (m+0>pm) pm=m+0} END {printf "%s  peak CPU %.1f%%  peak memory %.2f GiB\n", n, pc, pm/1024}' $f; done; done
```

## 7. Clean up (host)

```bash
sudo chown -R $USER:$USER ~/a3
docker rm -f node0 node1 node2
docker network rm a3net
```
