import java.util.Random

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{SQLContext, SaveMode}
import scopt.OptionParser
import util.{CyclicBuffer, ParItr, SparkApp}

import scala.collection.mutable.ArrayBuffer
import scala.util.Try
import com.github.fommil.netlib.BLAS.{getInstance => blas}
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.rdd._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{HashPartitioner, SparkContext}

import scala.collection.JavaConverters._


object SkipGram {
  val EXP_TABLE_SIZE = 1000
  val MAX_EXP = 6
  val UNIGRAM_TABLE_SIZE = 100000000
  val PART_TABLE_TOTAL_SIZE = 100000000

  def getPartition(i: Int, salt: Int, nPart: Int): Int = {
    var h = (i.toLong << 32) | salt
    h ^= h >>> 33
    h *= 0xff51afd7ed558ccdL
    h ^= h >>> 33
    h *= 0xc4ceb9fe1a85ec53L
    h ^= h >>> 33
    (Math.abs(h) % nPart).toInt
  }

  def createExpTable(): (Array[Float], Array[Float], Array[Float]) = {
    val expTable = new Array[Float](EXP_TABLE_SIZE)
    val expTableLog = new Array[Float](EXP_TABLE_SIZE)
    val expTable1mLog = new Array[Float](EXP_TABLE_SIZE)
    var i = 0
    while (i < EXP_TABLE_SIZE) {
      val tmp = math.exp((2.0 * i / EXP_TABLE_SIZE - 1.0) * MAX_EXP)
      expTable(i) = (tmp / (tmp + 1.0)).toFloat
      expTableLog(i) = Math.log(expTable(i)).toFloat
      expTable1mLog(i) = Math.log(1 - expTable(i)).toFloat
      i += 1
    }
    (expTable, expTableLog, expTable1mLog)
  }

  def createPartTable(nParts: Int): Array[Array[Int]] = {
    val nBuckets = PART_TABLE_TOTAL_SIZE / nParts

    val result = Array.fill(nBuckets)(Array.fill(nParts)(-1))
    (0 until nBuckets).foreach{bucket =>
      var i = 0
      val arr = Array.fill(nParts)(false)
      var n = 0
      while (n < nParts) {
        val p = SkipGram.getPartition(bucket, i, nParts)
        if (!arr(p)) {
          result(bucket)(n) = p
          arr(p) = true
          n += 1
        }
        i += 1
      }
    }
    result
  }

  private def shuffle(l: ArrayBuffer[Int], r: ArrayBuffer[Int], rnd: java.util.Random): Unit = {
    var i = 0
    val n = l.length
    var t = 0
    while (i < n - 1) {
      val j = i + rnd.nextInt(n - i)
      t = l(j)
      l(j) = l(i)
      l(i) = t

      t = r(j)
      r(j) = r(i)
      r(i) = t

      i += 1
    }
  }

  private def grouped(inIt: Iterator[Array[Int]], maxLen: Int): Iterator[Array[Array[Int]]] = {
    val it = inIt.buffered
    new Iterator[Array[Array[Int]]] {
      val data = ArrayBuffer.empty[Array[Int]]

      override def hasNext: Boolean = it.hasNext

      override def next(): Array[Array[Int]] = {
        var sum = 0
        data.clear()
        while (it.hasNext && (sum + it.head.length < maxLen || data.isEmpty)) {
          data.append(it.head)
          sum += it.head.length
          it.next()
        }
        data.toArray
      }
    }
  }

  def pairs(sent: RDD[Array[Int]],
            inSeed: Int,
            partitioner1: HashPartitioner,
            partitioner2: HashPartitioner,
            sampleProbBC: Broadcast[Int2IntOpenHashMap],
            window: Int,
            numPartitions: Int,
            numThread: Int): RDD[(Int, (Array[Int], Array[Int]))] = {
    sent.mapPartitions { it =>
      ParItr.map(SkipGram.grouped(it, 1000000), numThread) { sG =>
        val l = Array.fill(numPartitions)(ArrayBuffer.empty[Int])
        val r = Array.fill(numPartitions)(ArrayBuffer.empty[Int])

        val buffers1 = Array.fill(numPartitions)(new CyclicBuffer(window))
        val buffers2 = Array.fill(numPartitions)(new CyclicBuffer(window))
        val sampleProb = if (sampleProbBC != null) sampleProbBC.value else null
        var v = 0
        val random = new java.util.Random()
        var seed = inSeed
        sG.foreach { s =>
          var i = 0
          var skipped = 0
          random.setSeed(seed)
          while (i < s.length) {
            val prob = if (sampleProb != null && sampleProb.size() > 0) {
              sampleProb.getOrDefault(s(i), Int.MaxValue)
            } else {
              Int.MaxValue
            }

            if (prob < Int.MaxValue && prob < random.nextInt()) {
              skipped += 1
            } else {
              val a = partitioner1.getPartition(s(i))
              val b = partitioner2.getPartition(s(i))

              if (true) {
                val buffer = buffers2(a)
                buffer.checkVersion(v)
                buffer.resetIter()
                while (buffer.headInd() != -1 && buffer.headInd() >= (i - skipped) - window) {
                  val w = buffer.headVal()
                  if (s(i) != w) {
                    l(a).append(s(i))
                    r(a).append(w)
                  }

                  buffer.next()
                }
              }

              if (true) {
                val buffer = buffers1(b)
                buffer.checkVersion(v)
                buffer.resetIter()
                while (buffer.headInd() != -1 && buffer.headInd() >= (i - skipped) - window) {
                  val w = buffer.headVal()
                  if (s(i) != w) {
                    l(b).append(w)
                    r(b).append(s(i))
                  }
                  buffer.next()
                }
              }
              buffers1(a).checkVersion(v)
              buffers1(a).push(s(i), i - skipped)

              buffers2(b).checkVersion(v)
              buffers2(b).push(s(i), i - skipped)
            }
            seed = seed * 239017 + s(i)
            i += 1
          }
          v += 1
        }
        (0 until numPartitions).map{i =>
          shuffle(l(i), r(i), random)
          i -> (l(i).toArray, r(i).toArray)
        }
      }.flatten
    }
  }


  def listFiles(path: String): Array[String] = {
    val hdfs = FileSystem.get(new Configuration())
    Try(hdfs.listStatus(new Path(path)).map(_.getPath.getName)).getOrElse(Array.empty)
  }
}

class SkipGram extends Serializable with Logging {

  private var vectorSize: Int = 100
  private var window: Int = 5
  private var negative: Int = 5
  private var numIterations: Int = 1
  private var learningRate: Double = 0.025
  private var minLearningRate: Option[Double] = None
  private var minCount: Int = 1
  private var numThread: Int = 1
  private var numPartitions: Int = 1
  private var sample: Double = 0
  private var pow: Double = 0
  private var intermediateRDDStorageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK
  private var checkpointPath: String = null
  private var checkpointInterval: Int = 0

  def setVectorSize(vectorSize: Int): this.type = {
    require(vectorSize > 0,
      s"vector size must be positive but got ${vectorSize}")
    this.vectorSize = vectorSize
    this
  }

  def setLearningRate(learningRate: Double): this.type = {
    require(learningRate > 0,
      s"Initial learning rate must be positive but got ${learningRate}")
    this.learningRate = learningRate
    this
  }

  def setMinLearningRate(minLearningRate: Option[Double]): this.type = {
    require(minLearningRate.forall(_ > 0),
      s"Initial learning rate must be positive but got ${minLearningRate}")
    this.minLearningRate = minLearningRate
    this
  }

  def setNumPartitions(numPartitions: Int): this.type = {
    require(numPartitions > 0,
      s"Number of partitions must be positive but got ${numPartitions}")
    this.numPartitions = numPartitions
    this
  }

  def setCheckpointPath(path: String): this.type = {
    this.checkpointPath = path
    this
  }

  def setCheckpointInterval(interval: Int): this.type = {
    this.checkpointInterval = interval
    this
  }

  def setNumIterations(numIterations: Int): this.type = {
    require(numIterations >= 0,
      s"Number of iterations must be nonnegative but got ${numIterations}")
    this.numIterations = numIterations
    this
  }

  def setWindowSize(window: Int): this.type = {
    require(window > 0,
      s"Window of words must be positive but got ${window}")
    this.window = window
    this
  }


  def setPow(pow: Double): this.type = {
    require(pow >= 0,
      s"Pow must be positive but got ${pow}")
    this.pow = pow
    this
  }

  def setSample(sample: Double): this.type = {
    require(sample >= 0 && sample <= 1,
      s"sample must be between 0 and 1 but got $sample")
    this.sample = sample
    this
  }

  def setMinCount(minCount: Int): this.type = {
    require(minCount >= 0,
      s"Minimum number of times must be nonnegative but got ${minCount}")
    this.minCount = minCount
    this
  }

  def setNegative(negative: Int): this.type = {
    require(negative >= 0,
      s"Number of negative samples ${negative}")
    this.negative = negative
    this
  }

  def setNumThread(numThread: Int): this.type = {
    require(numThread >= 0,
      s"Number of threads ${numThread}")
    this.numThread = numThread
    this
  }

  def setIntermediateRDDStorageLevel(storageLevel: StorageLevel): this.type = {
    require(storageLevel != StorageLevel.NONE,
      "SkipGram is not designed to run without persisting intermediate RDDs.")
    this.intermediateRDDStorageLevel = storageLevel
    this
  }

  private def cacheAndCount[T](rdd: RDD[T]): RDD[T] = {
    val r = rdd.persist(intermediateRDDStorageLevel)
    r.count()
    r
  }

  def checkpoint(emb: RDD[(Int, (Long, Array[Float], Array[Float]))],
                 path: String)(implicit sc: SparkContext): RDD[(Int, (Long, Array[Float], Array[Float]))] = {
    val sqlc = new SQLContext(sc)
    import sqlc.implicits._
    if (emb != null) {
      emb.toDF("user_id", "emb").write.mode(SaveMode.Overwrite).parquet(path)
      emb.unpersist()
    }
    cacheAndCount(sqlc.read.parquet(path)
      .as[(Int, (Long, Array[Float], Array[Float]))]
      .rdd
    )
  }

  private def initUnigramTable(cn: ArrayBuffer[Long]): Array[Int] = {
    import SkipGram.UNIGRAM_TABLE_SIZE
    var a = 0
    var trainWordsPow = 0.0
    val table = Array.fill(UNIGRAM_TABLE_SIZE)(-1)
    while (a < cn.length) {
      trainWordsPow += Math.pow(cn(a), pow)
      a += 1
    }
    var i = 0
    a = 0
    var d1 = Math.pow(cn(i), pow) / trainWordsPow
    while (a < table.length && i < cn.length) {
      table(a) = i
      if (a.toDouble / table.length > d1) {
        i += 1
        d1 += Math.pow(cn(i), pow) / trainWordsPow
      }
      a += 1
    }
    table
  }

  def fit(dataset: RDD[Array[Int]]): RDD[(Int, (Long, Array[Float], Array[Float]))] = {
    import SkipGram.{createExpTable, createPartTable}

    assert(!((checkpointInterval > 0) ^ (checkpointPath != null)))

    val sc = dataset.context

    val countRDD = cacheAndCount(dataset.flatMap(identity(_)).map(_ -> 1L)
      .reduceByKey(_ + _).filter(_._2 >= minCount))

    val sampleProbBC = {
      val sampleProb = new Int2IntOpenHashMap()
      val trainWordsCount = countRDD.map(_._2).reduce(_ + _)
      countRDD.map{ case (v, n) =>
        val prob = if (sample > 0) {
          (Math.sqrt(n / (sample * trainWordsCount)) + 1) * (sample * trainWordsCount) / n
        } else {
          1.0
        }
        (v, if (prob >= 1) Int.MaxValue else (prob * Int.MaxValue).toInt)
      }.filter(_._2 < Int.MaxValue).collect().foreach{case (i, p) =>
        if (p < Int.MaxValue) {
          sampleProb.put(i, p)
        }
      }
      sc.broadcast(sampleProb)
    }

    val sent = cacheAndCount(dataset.repartition(numPartitions * 5))
    val expTable = sc.broadcast(createExpTable())
    val partTable = sc.broadcast(createPartTable(numPartitions))

    try {
      doFit(sent, countRDD, sampleProbBC, sc, expTable, partTable)
    } finally {
      // expTable.destroy()
      // sampleProbBC.destroy()
      sent.unpersist()
      countRDD.unpersist()
    }
  }

  private def doFit(sent: RDD[Array[Int]],
                    countRDD: RDD[(Int, Long)],
                    sampleProbBC: Broadcast[Int2IntOpenHashMap],
                    sc: SparkContext,
                    expTable: Broadcast[(Array[Float], Array[Float], Array[Float])],
                    partTable: Broadcast[Array[Array[Int]]]
                   ): RDD[(Int, (Long, Array[Float], Array[Float]))] = {
    import SkipGram._

    val latest = if (checkpointPath != null) {
      listFiles(checkpointPath)
        .filter(!_.contains("run_params")).filter(_.contains("_"))
        .map(_.split("_").map(_.toInt)).map{case Array(a, b) => (a, b)}
        .sorted.lastOption
    } else {
      None
    }

    latest.foreach(x => println(s"Continue training from epoch = ${x._1}, iteration = ${x._2}"))

    var emb = latest.map(x => checkpoint(null, checkpointPath + "/" + x._1 + "_" + x._2)(sent.sparkContext))
      .getOrElse{cacheAndCount(countRDD.mapPartitions{it =>
        val rnd = new Random(0)
        it.map{case (w, n) =>
          rnd.setSeed(w.hashCode)
          w -> (n, Array.fill(vectorSize)(((rnd.nextFloat() - 0.5) / vectorSize).toFloat),
            Array.fill(vectorSize)(((rnd.nextFloat() - 0.5) / vectorSize).toFloat))
        }
      })}.filter(_._2._1 >= minCount)

    var checkpointIter = 0
    val (startEpoch, startIter) = latest.getOrElse((0, 0))

    (startEpoch until numIterations).foreach {curEpoch =>
      ((if (curEpoch == startEpoch) startIter else 0) until numPartitions).foreach { pI =>
        val salt = curEpoch
        val progress = (1.0 * curEpoch.toDouble * numPartitions + pI) / (numIterations * numPartitions)
        val curLearningRate = minLearningRate.fold(learningRate)(e => Math.exp(Math.log(learningRate) - (Math.log(learningRate) - Math.log(e)) * progress))
        val partitioner1 = new HashPartitioner(numPartitions) {
          override def getPartition(key: Any): Int = {
            SkipGram.getPartition(key.asInstanceOf[Int], salt, numPartitions)
          }
        }

        val partitioner2 = new HashPartitioner(numPartitions) {
          override def getPartition(key: Any): Int = {
            val bucket = SkipGram.getPartition(key.asInstanceOf[Int], curEpoch, partTable.value.length)
            partTable.value.apply(bucket).apply(pI)
          }
        }

        val partitionerKey = new HashPartitioner(numPartitions) {
          override def getPartition(key: Any): Int = key.asInstanceOf[Int]
        }

        val embL = emb.map(x => x._1 -> (x._2._1, x._2._2)).partitionBy(partitioner1)
        val embR = emb.map(x => x._1 -> (x._2._1, x._2._3)).partitionBy(partitioner2)

        val cur = pairs(sent, curEpoch * numPartitions + pI, partitioner1, partitioner2, sampleProbBC, window,
          numPartitions, numThread).partitionBy(partitionerKey).values

        val loss = sc.doubleAccumulator
        val lossN = sc.longAccumulator

        val newEmb = cacheAndCount(cur.zipPartitions(embL, embR) { case (sIt, eItL, eItR) =>
          val vocabL = new Int2IntOpenHashMap()
          val vocabR = new Int2IntOpenHashMap()
          val rSyn0 = ArrayBuffer.empty[Array[Float]]
          val rSyn1Neg = ArrayBuffer.empty[Array[Float]]
          var seed = 0
          val cnL = ArrayBuffer.empty[Long]
          val cnR = ArrayBuffer.empty[Long]

          eItL.foreach { case (v, (n, f)) =>
            val i = vocabL.size
            vocabL.put(v, i)
            cnL.append(n)
            rSyn0 += f
            seed = seed * 239017 + v
          }

          eItR.foreach { case (v, (n, f)) =>
            val i = vocabR.size
            vocabR.put(v, i)
            cnR.append(n)
            rSyn1Neg += f
            seed = seed * 239017 + v
          }

          val table = if (pow > 0) {
            initUnigramTable(cnR)
          } else {
            Array.empty[Int]
          }

          val syn0 = Array.fill(rSyn0.length * vectorSize)(0f)
          rSyn0.iterator.zipWithIndex.foreach { case (f, i) =>
            System.arraycopy(f, 0, syn0, i * vectorSize, vectorSize)
          }
          rSyn0.clear()

          val syn1Neg = Array.fill(rSyn1Neg.length * vectorSize)(0f)
          rSyn1Neg.iterator.zipWithIndex.foreach { case (f, i) =>
            System.arraycopy(f, 0, syn1Neg, i * vectorSize, vectorSize)
          }
          rSyn1Neg.clear()
          val lExpTable = expTable.value
          val random = new java.util.Random(seed)

          ParItr.foreach(sIt, numThread)({ case ((l, r)) =>
            var lLoss = 0.0
            var lLossN = 0L
            var pos = 0
            var word = -1
            var lastWord = -1
            while (pos < l.length) {
              lastWord = vocabL.getOrDefault(l(pos), -1)
              word = vocabR.getOrDefault(r(pos), -1)

              if (word != -1 && lastWord != -1) {
                val l1 = lastWord * vectorSize
                val neu1e = new Array[Float](vectorSize)
                var target = -1
                var label = -1
                var d = 0
                while (d < negative + 1) {
                  if (d == 0) {
                    target = word
                    label = 1
                  } else {
                    if (pow > 0) {
                      target = table(random.nextInt(table.length))
                      while (target == word || target == -1) {
                        target = table(random.nextInt(table.length))
                      }
                    } else {
                      target = random.nextInt(vocabR.size)
                      while (target == word) {
                        target = random.nextInt(vocabR.size)
                      }
                    }
                    label = 0
                  }
                  val l2 = target * vectorSize
                  val f = blas.sdot(vectorSize, syn0, l1, 1, syn1Neg, l2, 1)
                  var g = 0.0
                  var sigm = 0.0
                  if (f > MAX_EXP) {
                    sigm = 1.0
                    lLoss += (-(if (label > 0) 0 else -6.00247569))
                    lLossN += 1
                  } else if (f < -MAX_EXP) {
                    sigm = 0.0
                    lLoss += (-(if (label > 0) -6.00247569 else 0))
                    lLossN += 1
                  } else {
                    val ind = ((f + MAX_EXP) * (EXP_TABLE_SIZE / MAX_EXP / 2.0)).toInt
                    sigm = lExpTable._1(ind)
                    lLoss += (-(if (label > 0) lExpTable._2(ind) else lExpTable._3(ind)))
                    lLossN += 1
                  }
                  g = (label - sigm) * curLearningRate
                  blas.saxpy(vectorSize, g.toFloat, syn1Neg, l2, 1, neu1e, 0, 1)
                  blas.saxpy(vectorSize, g.toFloat, syn0, l1, 1, syn1Neg, l2, 1)
                  d += 1
                }
                blas.saxpy(vectorSize, 1.0f, neu1e, 0, 1, syn0, l1, 1)
              }
              pos += 1
            }
            loss.synchronized(loss.add(lLoss))
            lossN.synchronized(lossN.add(lLossN))
          })

          vocabL.int2IntEntrySet().fastIterator().asScala.map { e =>
            val k = e.getIntKey
            val v = e.getIntValue
            0 -> (k -> (cnL(v), syn0.slice(v * vectorSize, (v + 1) * vectorSize)))
          } ++ vocabR.int2IntEntrySet().fastIterator().asScala.map { e =>
            val k = e.getIntKey
            val v = e.getIntValue
            1 -> (k -> (cnR(v), syn1Neg.slice(v * vectorSize, (v + 1) * vectorSize)))
          }
        })
        emb.unpersist()
        emb = cacheAndCount(newEmb.filter(_._1 == 0).values.join(newEmb.filter(_._1 == 1).values).mapValues(x => (x._1._1, x._1._2, x._2._2)))
        newEmb.unpersist()

        println("LOSS: " + loss.sum / lossN.sum + " (" + loss.sum + " / " + lossN.sum + ")")

        if (checkpointInterval > 0 && (checkpointIter + 1) % checkpointInterval == 0) {
          emb = checkpoint(emb, checkpointPath + "/" + curEpoch + "_" + (pI + 1))(sent.sparkContext)
        }
        checkpointIter += 1
      }
      if (checkpointInterval > 0) {
        emb = checkpoint(emb, checkpointPath + "/" + curEpoch)(sent.sparkContext)
      }

      sent.unpersist()
      emb
    }
    emb
  }
}

case class SkipGramParams(input: String = null,
                          output: String = null,
                          dim: Int = -1,
                          negative: Int = 5,
                          window: Int = 5,
                          epoch: Int = 1,
                          alpha: Double = 0.025,
                          minAlpha: Option[Double] = None,
                          minCount: Int = 5,
                          sample: Option[Double] = None,
                          pow: Option[Double] = None,
                          checkpointInterval: Int = 0) {
  override def toString: String = {
    classOf[SkipGramParams].getDeclaredFields.map{x =>
      x.getName + ": " + x.get(this).toString
    }.toIterator.mkString("\n")
  }
}


object SkipGramRun extends SparkApp[SkipGramParams] {
  override def parser: Seq[String] => Option[SkipGramParams] = new OptionParser[SkipGramParams]("SkipGram") {
    opt[String]("input").action((x, c) => c.copy(input = x))
    opt[String]("output").action((x, c) => c.copy(output = x))
    opt[Int]("dim").action((x, c) => c.copy(dim = x))
    opt[Int]("negative").action((x, c) => c.copy(negative = x))
    opt[Int]("window").action((x, c) => c.copy(window = x))
    opt[Int]("epoch").action((x, c) => c.copy(epoch = x))
    opt[Double]("alpha").action((x, c) => c.copy(alpha = x))
    opt[Double]("minAlpha").optional().action((x, c) => c.copy(minAlpha = Some(x)))
    opt[Int]("minCount").action((x, c) => c.copy(minCount = x))
    opt[Double]("pow").optional().action((x, c) => c.copy(pow = Some(x)))
    opt[Double]("sample").optional().action((x, c) => c.copy(sample = Some(x)))
    opt[Int]("checkpointInterval").action((x, c) => c.copy(checkpointInterval = x))
  }.parse(_, SkipGramParams())

  def rm(path: String): Unit = {
    val hdfs = FileSystem.get(new Configuration())
    try { hdfs.delete(new Path(path), true) } catch { case _ : Throwable =>}
  }

  override def run(params: SkipGramParams)(implicit sc: SparkContext, sqlc: SQLContext): Unit = {
    import sqlc.implicits._
    val start = System.currentTimeMillis()

    rm(params.output + "/run_params")
    sc.parallelize(Seq(params.toString), 1).saveAsTextFile(params.output + "/run_params")

    val data = sqlc.read.parquet(params.input)
      .select(org.apache.spark.sql.functions.col("walk"))
      .filter(org.apache.spark.sql.functions.size(org.apache.spark.sql.functions.col("walk")) > 1)
      .as[Array[Int]]
      .rdd

    val numExecutors = sc.getConf.get("spark.executor.instances").toInt
    val numCores = sc.getConf.get("spark.executor.cores").toInt

    new SkipGram()
      .setVectorSize(params.dim)
      .setNegative(params.negative)
      .setWindowSize(params.window)
      .setNumIterations(params.epoch)
      .setLearningRate(params.alpha)
      .setMinLearningRate(params.minAlpha)
      .setMinCount(params.minCount)
      .setPow(params.pow.getOrElse(0))
      .setSample(params.sample.getOrElse(0))
      .setIntermediateRDDStorageLevel(StorageLevel.DISK_ONLY_2)
      .setNumThread(numCores)
      .setNumPartitions(numExecutors)
      .setCheckpointPath(params.output)
      .setCheckpointInterval(params.checkpointInterval)
      .fit(data)

    println(System.currentTimeMillis() - start)
  }
}