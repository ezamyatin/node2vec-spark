# node2vec-spark

Billion-scale Node2Vec implementation with custom Word2Vec-SkipGram.

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
--q 10 \
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
--dim 200 \
--negative 10 \
--window 2 \
--epoch 10 \
--alpha 0.025 \
--minAlpha 0.0001 \
--minCount 10 \
--pow 0 \
--checkpointInterval 30
```
