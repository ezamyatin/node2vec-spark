# node2vec-spark

Billion-scale Node2Vec implementation with custom Word2Vec-SkipGram.

## Benchmark
* Number of vertices: 100M
* Total walks length: 250B
* Allocated resources: 1250 cores, 3.7Tb RAM

**RandomWalk:**
  * num-walks: 25
  * length: 100
  * running-time: **5h**

**SkipGram:**
  * epochs: 5
  * window-size: 2
  * negative: 10
  * dim: 128
  * running-time: **80h**
  

## Build and Run

Build
```
sbt assembly
```

Run
```
spark-submit --master yarn \
--deploy-mode cluster \
--class RandomWalk \
--driver-memory <driver memoroy> \
--executor-memory <executor-memory> \
--executor-cores <executor-cores> \
--num-executors <num-executors> \
friends_suggestion-assembly-1.0.jar \
--input <input> \
--output <output> \
--numWalks 25 \
--length 100 \
--p 1 \
--q 1 \
--checkpointInterval 10
```

```
spark-submit --master yarn \
--deploy-mode cluster \
--class SkipGramRun \
--driver-memory <driver memoroy> \
--executor-memory <executor-memory> \
--executor-cores <num threads> \
--num-executors <num-executors> \
--conf spark.task.cpus=<num threads> \
friends_suggestion-assembly-1.0.jar \
--input <input> \
--output <output> \
--dim <dim> \
--negative <negative> \
--window <window> \
--epoch <epoch> \
--alpha <alpha> \
--minAlpha <minAlpha> \
--minCount <minCount> \
--pow <pow> \
--sample <sample> \
--checkpointInterval <checkpointInterval> \
```
