# node2vec-spark

Billion-scale Node2Vec implementation with custom Word2Vec-SkipGram.

## Build and Run

```console
sbt assembly

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
