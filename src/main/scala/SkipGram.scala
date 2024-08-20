import java.util.Random

import util.{CyclicBuffer, ParItr, SparkApp}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{SQLContext, SaveMode}

import scala.collection.mutable.ArrayBuffer
import scala.util.Try
import com.github.fommil.netlib.BLAS.{getInstance => blas}
import com.google.common.util.concurrent.AtomicDouble
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.rdd._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{HashPartitioner, SparkContext}

import scala.collection.JavaConverters._
import java.util.concurrent.atomic.AtomicLong

import SkipGram.{Context, ItemID, ItemID2IntMap, SamplingMode}
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap
import scopt.OptionParser


object SkipGram {
  type ItemID = Int
  type ItemID2IntMap = Int2IntOpenHashMap

  val EXP_TABLE_SIZE = 1000
  val MAX_EXP = 6
  val UNIGRAM_TABLE_SIZE = 100000000
  val PART_TABLE_TOTAL_SIZE = 100000000

  object SamplingMode {
    val WINDOW = 0
    val SAMPLE = 1
    val SAMPLE_POS2NEG = 2
  }

  case class Context(vocabL: ItemID2IntMap, vocabR: ItemID2IntMap,
                     cnL: Array[Long], cnR: Array[Long],
                     syn0: Array[Float], syn1Neg: Array[Float], table: Array[Int],
                     expTable: (Array[Float], Array[Float], Array[Float]),
                     loss: AtomicDouble, lossn: AtomicLong,
                     random: Random)

  def getPartition(i: ItemID, salt: Int, nPart: Int): Int = {
    var h = (i.hashCode().toLong << 32) | salt
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

  private def shuffle(l: ArrayBuffer[ItemID], r: ArrayBuffer[ItemID], rnd: java.util.Random): Unit = {
    var i = 0
    val n = l.length
    var t = 0.asInstanceOf[ItemID]
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

  def skipPair(i: ItemID, j: ItemID): Boolean = {
    i == j
  }

  def pairsSamplePos2Neg(inIt: Iterator[Array[ItemID]],
                         samplingMode: Int,
                         inSeed: Int,
                         partitioner1: HashPartitioner,
                         partitioner2: HashPartitioner,
                         window: Int,
                         numPartitions: Int): Iterator[(Int, (Array[ItemID], Array[ItemID]))] = {

    new Iterator[(Int, (Array[ItemID], Array[ItemID]))] {
      val batchSize = 1000000 / numPartitions
      val l = Array.fill(numPartitions)(new ArrayBuffer[ItemID](batchSize))
      val r = Array.fill(numPartitions)(new ArrayBuffer[ItemID](batchSize))
      val partitionb = new ArrayBuffer[Int](1000)

      val it = inIt.buffered

      val random = new java.util.Random()
      var seed = inSeed.asInstanceOf[ItemID]

      var filled = -1
      var lastPtr = 0
      var nonEmptyCounter = 0
      var i = 0

      override def hasNext: Boolean = {
        it.hasNext || nonEmptyCounter > 0
      }

      def fill(): Unit = {
        assert(filled < 0)
        while (it.hasNext && filled < 0) {
          random.setSeed(seed)
          val inS = it.head
          val s1 = inS.filter(_ > 0)
          val s2 = inS.filter(_ < 0)

          if (partitionb.isEmpty) {
            s2.foreach(w => partitionb.append(partitioner2.getPartition(w)))
          }

          while (i < s1.length && filled < 0) {
            val a = partitioner1.getPartition(s1(i))

            if (true) {
              var j = 0
              val n = Math.min(2 * window, s2.length - 1)
              while (j < n) {
                val c = random.nextInt(s2.length)

                if (!skipPair(s1(i), s2(c)) && partitionb(c) == a) {
                  if (l(a).isEmpty) {
                    nonEmptyCounter += 1
                  }

                  l(a).append(s1(i))
                  r(a).append(s2(c))

                  filled = if (l(a).length < batchSize) -1 else a
                }
                j += 1
              }
            }
            seed = seed * 239017 + s1(i)
            i += 1
          }

          if (i == s1.length) {
            it.next()
            partitionb.clear()
            i = 0
          }
        }
      }

      override def next(): (Int, (Array[ItemID], Array[ItemID])) = {
        while (it.hasNext && filled < 0) {
          fill()
        }

        if (filled >= 0) {
          shuffle(l(filled), r(filled), random)
          val res = (filled, (l(filled).toArray, r(filled).toArray))
          l(filled).clear()
          r(filled).clear()
          filled = -1
          nonEmptyCounter -= 1
          res
        } else {
          var res = null.asInstanceOf[(Int, (Array[ItemID], Array[ItemID]))]
          while (lastPtr < l.length && res == null) {
            if (l(lastPtr).nonEmpty) {
              shuffle(l(lastPtr), r(lastPtr), random)

              res = (lastPtr, (l(lastPtr).toArray, r(lastPtr).toArray))
              nonEmptyCounter -= 1
              l(lastPtr).clear()
              r(lastPtr).clear()
            }
            lastPtr += 1
          }

          res
        }
      }
    }
  }


  def pairsSample(inIt: Iterator[Array[ItemID]],
                  samplingMode: Int,
                  inSeed: Int,
                  partitioner1: HashPartitioner,
                  partitioner2: HashPartitioner,
                  window: Int,
                  numPartitions: Int): Iterator[(Int, (Array[ItemID], Array[ItemID]))] = {
    assert(samplingMode == SamplingMode.SAMPLE)

    new Iterator[(Int, (Array[ItemID], Array[ItemID]))] {
      val batchSize = 1000000 / numPartitions
      val l = Array.fill(numPartitions)(new ArrayBuffer[ItemID](batchSize))
      val r = Array.fill(numPartitions)(new ArrayBuffer[ItemID](batchSize))
      val partitionb = new ArrayBuffer[Int](1000)

      val it = inIt.buffered

      val random = new java.util.Random()
      var seed = inSeed.asInstanceOf[ItemID]

      var filled = -1
      var lastPtr = 0
      var nonEmptyCounter = 0
      var i = 0

      override def hasNext: Boolean = {
        it.hasNext || nonEmptyCounter > 0
      }

      def fill(): Unit = {
        assert(filled < 0)
        while (it.hasNext && filled < 0) {
          random.setSeed(seed)
          val s = it.head
          if (partitionb.isEmpty) {
            s.foreach(w => partitionb.append(partitioner2.getPartition(w)))
          }

          while (i < s.length && filled < 0) {
            val a = partitioner1.getPartition(s(i))

            if (true) {
              var j = 0
              val n = Math.min(2 * window, s.length - 1)
              while (j < n) {
                var c = i
                while (c == i) {
                  c = random.nextInt(s.length)
                }

                if (!skipPair(s(c), s(i)) && partitionb(c) == a) {
                  if (l(a).isEmpty) {
                    nonEmptyCounter += 1
                  }

                  l(a).append(s(i))
                  r(a).append(s(c))

                  filled = if (l(a).length < batchSize) -1 else a
                }
                j += 1
              }
            }
            seed = seed * 239017 + s(i)
            i += 1
          }

          if (i == s.length) {
            it.next()
            partitionb.clear()
            i = 0
          }
        }
      }

      override def next(): (Int, (Array[ItemID], Array[ItemID])) = {
        while (it.hasNext && filled < 0) {
          fill()
        }

        if (filled >= 0) {
          shuffle(l(filled), r(filled), random)
          val res = (filled, (l(filled).toArray, r(filled).toArray))
          l(filled).clear()
          r(filled).clear()
          filled = -1
          nonEmptyCounter -= 1
          res
        } else {
          var res = null.asInstanceOf[(Int, (Array[ItemID], Array[ItemID]))]
          while (lastPtr < l.length && res == null) {
            if (l(lastPtr).nonEmpty) {
              shuffle(l(lastPtr), r(lastPtr), random)

              res = (lastPtr, (l(lastPtr).toArray, r(lastPtr).toArray))
              nonEmptyCounter -= 1
              l(lastPtr).clear()
              r(lastPtr).clear()
            }
            lastPtr += 1
          }

          res
        }
      }
    }
  }

  def pairsWindow(inIt: Iterator[Array[ItemID]],
                  samplingMode: Int,
                  inSeed: Int,
                  partitioner1: HashPartitioner,
                  partitioner2: HashPartitioner,
                  window: Int,
                  numPartitions: Int): Iterator[(Int, (Array[ItemID], Array[ItemID]))] = {

    new Iterator[(Int, (Array[ItemID], Array[ItemID]))] {
      val batchSize = 1000000 / numPartitions
      val l = Array.fill(numPartitions)(new ArrayBuffer[ItemID](batchSize))
      val r = Array.fill(numPartitions)(new ArrayBuffer[ItemID](batchSize))

      val it = inIt.buffered

      val buffers1 = Array.fill(numPartitions)(new CyclicBuffer(window))
      val buffers2 = Array.fill(numPartitions)(new CyclicBuffer(window))
      var v = 0
      val random = new java.util.Random()
      var seed = inSeed.asInstanceOf[ItemID]

      var filleda = -1
      var filledb = -1
      var lastPtr = 0
      var nonEmptyCounter = 0

      var i = 0
      var skipped = 0

      override def hasNext: Boolean = {
        it.hasNext || nonEmptyCounter > 0
      }

      def fill(): Unit = {
        assert(filleda < 0 && filledb < 0)
        while (it.hasNext && filleda < 0 && filledb < 0) {
          random.setSeed(seed)
          val s = it.head
          while (i < s.length && filleda < 0 && filledb < 0) {

            val a = partitioner1.getPartition(s(i))
            val b = partitioner2.getPartition(s(i))

            if (true) {
              val buffer = buffers2(a)
              buffer.checkVersion(v)
              buffer.resetIter()
              while (buffer.headInd() != -1 && buffer.headInd() >= (i - skipped) - window) {
                val w = buffer.headVal()
                if (!skipPair(s(i), w)) {
                  if (l(a).isEmpty) {
                    nonEmptyCounter += 1
                  }

                  l(a).append(s(i))
                  r(a).append(w)

                  filleda = if (l(a).length < batchSize) -1 else a
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
                if (!skipPair(s(i), w)) {
                  if (l(b).isEmpty) {
                    nonEmptyCounter += 1
                  }

                  l(b).append(w)
                  r(b).append(s(i))

                  filledb = if (l(b).length < batchSize) -1 else b
                }
                buffer.next()
              }
            }

            buffers1(a).checkVersion(v)
            buffers1(a).push(s(i), i - skipped)

            buffers2(b).checkVersion(v)
            buffers2(b).push(s(i), i - skipped)
            seed = seed * 239017 + s(i)
            i += 1
          }

          if (i == s.length) {
            it.next()
            i = 0
            skipped = 0
            v += 1
          }
        }
      }

      override def next(): (Int, (Array[ItemID], Array[ItemID])) = {
        while (it.hasNext && filleda < 0 && filledb < 0) {
          fill()
        }

        if (filleda >= 0) {
          shuffle(l(filleda), r(filleda), random)
          val res = (filleda, (l(filleda).toArray, r(filleda).toArray))
          l(filleda).clear()
          r(filleda).clear()
          filleda = -1
          nonEmptyCounter -= 1
          res
        } else if (filledb >= 0) {
          shuffle(l(filledb), r(filledb), random)
          val res = (filledb, (l(filledb).toArray, r(filledb).toArray))
          l(filledb).clear()
          r(filledb).clear()
          filledb = -1
          nonEmptyCounter -= 1
          res
        } else {
          var res = null.asInstanceOf[(Int, (Array[ItemID], Array[ItemID]))]
          while (lastPtr < l.length && res == null) {
            if (l(lastPtr).nonEmpty) {
              shuffle(l(lastPtr), r(lastPtr), random)

              res = (lastPtr, (l(lastPtr).toArray, r(lastPtr).toArray))
              nonEmptyCounter -= 1
              //l(lastPtr).clear()
              //r(lastPtr).clear()
            }
            lastPtr += 1
          }

          //assert(res != null)
          res
        }
      }
    }
  }


  def listFiles(path: String): Array[String] = {
    val hdfs = FileSystem.get(new Configuration())
    Try(hdfs.listStatus(new Path(path)).map(_.getPath.getName)).getOrElse(Array.empty)
  }
}

class SkipGram extends Serializable with Logging {

  private var dotVectorSize: Int = 100
  private var window: Int = 5
  private var samplingMode: Int = 0
  private var negative: Int = 5
  private var numIterations: Int = 1
  private var learningRate: Double = 0.025
  private var minLearningRate: Option[Double] = None
  private var minCount: Int = 1
  private var numThread: Int = 1
  private var numPartitions: Int = 1
  private var pow: Double = 0
  private var lambda: Double = 0
  private var useBias: Boolean = false
  private var intermediateRDDStorageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK
  private var checkpointPath: String = null
  private var checkpointInterval: Int = 0

  def setVectorSize(vectorSize: Int): this.type = {
    require(vectorSize > 0,
      s"vector size must be positive but got ${vectorSize}")
    this.dotVectorSize = vectorSize
    this
  }

  def vectorSize: Int = if (!useBias) dotVectorSize else dotVectorSize + 1

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

  def setSamplingMode(samplingMode: Int): this.type = {
    this.samplingMode = samplingMode
    this
  }

  def setUseBias(useBias: Boolean): this.type = {
    this.useBias = useBias
    this
  }

  def setPow(pow: Double): this.type = {
    require(pow >= 0,
      s"Pow must be positive but got ${pow}")
    this.pow = pow
    this
  }

  def setLambda(lambda: Double): this.type = {
    require(lambda >= 0,
      s"Lambda must be positive but got ${lambda}")
    this.lambda = lambda
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

  def checkpoint(emb: RDD[(ItemID, (Long, Array[Float], Array[Float]))],
                 path: String)(implicit sc: SparkContext): RDD[(ItemID, (Long, Array[Float], Array[Float]))] = {
    val sqlc = new SQLContext(sc)
    import sqlc.implicits._
    if (emb != null) {
      emb.toDF("user_id", "emb").write.mode(SaveMode.Overwrite).parquet(path)
      emb.unpersist()
    }
    cacheAndCount(sqlc.read.parquet(path)
      .as[(ItemID, (Long, Array[Float], Array[Float]))]
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

  private def initUnigramTable(cn: ArrayBuffer[Long],
                               indices: Array[Int]): Array[Int] = {
    import SkipGram.UNIGRAM_TABLE_SIZE
    var a = 0
    var trainWordsPow = 0.0
    val table = Array.fill(UNIGRAM_TABLE_SIZE)(-1)
    while (a < indices.length) {
      trainWordsPow += Math.pow(cn(indices(a)), pow)
      a += 1
    }
    var i = 0
    a = 0
    var d1 = Math.pow(cn(indices(i)), pow) / trainWordsPow
    while (a < table.length && i < indices.length) {
      table(a) = indices(i)
      if (a.toDouble / table.length > d1) {
        i += 1
        d1 += Math.pow(cn(indices(i)), pow) / trainWordsPow
      }
      a += 1
    }
    table
  }

  def optimize(l: Array[ItemID],
               r: Array[ItemID],
               curLearningRate: Double,
               context: SkipGram.Context): Unit = {
    import SkipGram._

    var loss = 0.0
    var lossn = 0L
    var pos = 0
    var word = -1
    var lastWord = -1
    while (pos < l.length) {
      lastWord = context.vocabL.getOrDefault(l(pos), -1)
      word = context.vocabR.getOrDefault(r(pos), -1)

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
            if (context.table.nonEmpty) {
              target = context.table(context.random.nextInt(context.table.length))
              while (skipPair(target, word) || target == -1) {
                target = context.table(context.random.nextInt(context.table.length))
              }
            } else {
              target = context.random.nextInt(context.vocabR.size)
              while (skipPair(target, word)) {
                target = context.random.nextInt(context.vocabR.size)
              }
            }
            label = 0
          }
          val l2 = target * vectorSize
          var f = blas.sdot(dotVectorSize, context.syn0, l1, 1, context.syn1Neg, l2, 1)
          if (useBias) {
            f += context.syn0(l1 + dotVectorSize)
            f += context.syn1Neg(l2 + dotVectorSize)
          }

          var g = 0.0
          var sigm = 0.0
          if (f > MAX_EXP) {
            sigm = 1.0
            loss += (-(if (label > 0) 0 else -6.00247569))
            lossn += 1
          } else if (f < -MAX_EXP) {
            sigm = 0.0
            loss += (-(if (label > 0) -6.00247569 else 0))
            lossn += 1
          } else {
            val ind = ((f + MAX_EXP) * (EXP_TABLE_SIZE / MAX_EXP / 2.0)).toInt
            sigm = context.expTable._1(ind)
            loss += (-(if (label > 0) context.expTable._2(ind) else context.expTable._3(ind)))
            lossn += 1
          }
          g = (label - sigm) * curLearningRate

          if (lambda > 0) {
            blas.saxpy(dotVectorSize, -(lambda * curLearningRate).toFloat, context.syn0, l1, 1, neu1e, 0, 1)
          }
          blas.saxpy(dotVectorSize, g.toFloat, context.syn1Neg, l2, 1, neu1e, 0, 1)
          if (useBias) {
            neu1e(dotVectorSize) += g.toFloat * 1
          }

          if (lambda > 0) {
            blas.saxpy(dotVectorSize, -(lambda * curLearningRate).toFloat, context.syn1Neg, l2, 1, context.syn1Neg, l2, 1)
          }
          blas.saxpy(dotVectorSize, g.toFloat, context.syn0, l1, 1, context.syn1Neg, l2, 1)
          if (useBias) {
            context.syn1Neg(l2 + dotVectorSize) += g.toFloat * 1
          }

          d += 1
        }
        blas.saxpy(vectorSize, 1.0f, neu1e, 0, 1, context.syn0, l1, 1)
      }
      pos += 1
    }

    context.loss.addAndGet(loss)
    context.lossn.addAndGet(lossn)
  }

  def initContext(eItLR: Iterator[(Int, ItemID, Long, Array[Float])],
                  expTable: (Array[Float], Array[Float], Array[Float])): SkipGram.Context = {
    val vocabL = new ItemID2IntMap()
    val vocabR = new ItemID2IntMap()
    val rSyn0 = ArrayBuffer.empty[Array[Float]]
    val rSyn1Neg = ArrayBuffer.empty[Array[Float]]
    var seed = 0.asInstanceOf[ItemID]
    val cnL = ArrayBuffer.empty[Long]
    val cnR = ArrayBuffer.empty[Long]
    val loss = new AtomicDouble(0)
    val lossn = new AtomicLong(0)

    eItLR.foreach { case (t, v, n, f) =>
      if (t == 0) {
        val i = vocabL.size
        vocabL.put(v, i)
        cnL.append(n)
        rSyn0 += f
        seed = seed * 239017 + v
      } else {
        val i = vocabR.size
        vocabR.put(v, i)
        cnR.append(n)
        rSyn1Neg += f
        seed = seed * 239017 + v
      }
    }

    val table = if (samplingMode == SamplingMode.SAMPLE_POS2NEG) {
      val indices = vocabR.keySet().iterator().asScala.filter(_ < 0).map(vocabR(_).toInt).toArray
      initUnigramTable(cnR, indices)
    } else if (pow > 0) {
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

    val random = new java.util.Random(seed)

    Context(vocabL, vocabR, cnL.toArray, cnR.toArray, syn0, syn1Neg, table, expTable, loss, lossn, random)
  }

  def fit(dataset: RDD[Array[ItemID]]): RDD[(ItemID, (Long, Array[Float], Array[Float]))] = {
    import SkipGram.{createExpTable, createPartTable}

    assert(!((checkpointInterval > 0) ^ (checkpointPath != null)))

    val sc = dataset.context

    val numExecutors = sc.getConf.get("spark.executor.instances").toInt
    val numCores = sc.getConf.get("spark.executor.cores").toInt

    val sent = cacheAndCount(dataset.repartition(numExecutors * numCores / numThread))
    val expTable = sc.broadcast(createExpTable())
    val partTable = sc.broadcast(createPartTable(numPartitions))

    try {
      doFit(sent, sc, expTable, partTable)
    } finally {
      // expTable.destroy()
      // sampleProbBC.destroy()
      sent.unpersist()
    }
  }

  private def doFit(sent: RDD[Array[ItemID]],
                    sc: SparkContext,
                    expTable: Broadcast[(Array[Float], Array[Float], Array[Float])],
                    partTable: Broadcast[Array[Array[Int]]]
                   ): RDD[(ItemID, (Long, Array[Float], Array[Float]))] = {
    import SkipGram._

    val latest = if (checkpointPath != null) {
      listFiles(checkpointPath)
        .filter(file => listFiles(checkpointPath + "/" + file).contains("_SUCCESS"))
        .filter(!_.contains("run_params")).filter(_.contains("_"))
        .map(_.split("_").map(_.toInt)).map{case Array(a, b) => (a, b)}
        .sorted.lastOption
    } else {
      None
    }

    latest.foreach(x => println(s"Continue training from epoch = ${x._1}, iteration = ${x._2}"))

    val countRDD = sent.flatMap(identity(_)).map(_ -> 1L)
      .reduceByKey(_ + _).filter(_._2 >= minCount)

    var emb = latest.map(x => checkpoint(null, checkpointPath + "/" + x._1 + "_" + x._2)(sent.sparkContext))
      .getOrElse{cacheAndCount(countRDD.mapPartitions{it =>
        val rnd = new Random(0)
        it.map{case (w, n) =>
          rnd.setSeed(w.hashCode)
          val emb = (n, Array.fill(vectorSize)(((rnd.nextFloat() - 0.5) / vectorSize).toFloat),
            Array.fill(vectorSize)(((rnd.nextFloat() - 0.5) / vectorSize).toFloat))
          if (useBias) {
            emb._2(dotVectorSize) = 0
            emb._3(dotVectorSize) = 0
          }
          w -> emb
        }
      })}.filter(_._2._1 >= minCount)

    var checkpointIter = 0
    val (startEpoch, startIter) = latest.getOrElse((0, 0))
    val cached = ArrayBuffer.empty[RDD[(ItemID, (Long, Array[Float], Array[Float]))]]

    (startEpoch until numIterations).foreach {curEpoch =>
      ((if (curEpoch == startEpoch) startIter else 0) until numPartitions).foreach { pI =>
        val salt = curEpoch
        val progress = (1.0 * curEpoch.toDouble * numPartitions + pI) / (numIterations * numPartitions)
        val curLearningRate = minLearningRate.fold(learningRate)(e => Math.exp(Math.log(learningRate) - (Math.log(learningRate) - Math.log(e)) * progress))
        val partitioner1 = new HashPartitioner(numPartitions) {
          override def getPartition(key: Any): Int = {
            SkipGram.getPartition(key.asInstanceOf[ItemID], salt, numPartitions)
          }
        }

        val partitioner2 = new HashPartitioner(numPartitions) {
          override def getPartition(key: Any): Int = {
            val bucket = SkipGram.getPartition(key.asInstanceOf[ItemID], curEpoch, partTable.value.length)
            partTable.value.apply(bucket).apply(pI)
          }
        }

        val partitionerKey = new HashPartitioner(numPartitions) {
          override def getPartition(key: Any): Int = key.asInstanceOf[Int]
        }

        val embLR = emb.flatMap{x =>
          Iterator(partitioner1.getPartition(x._1) -> (0, x._1, x._2._1, x._2._2), partitioner2.getPartition(x._1) -> (1, x._1, x._2._1, x._2._3))
        }.partitionBy(partitionerKey).values

        val cur = {
          if (samplingMode == SamplingMode.WINDOW) {
            sent.mapPartitions(pairsWindow(_, samplingMode, curEpoch * numPartitions + pI, partitioner1, partitioner2, window,
              numPartitions))
          } else if (samplingMode == SamplingMode.SAMPLE){
            sent.mapPartitions(pairsSample(_, samplingMode, curEpoch * numPartitions + pI, partitioner1, partitioner2, window,
              numPartitions))
          } else if (samplingMode == SamplingMode.SAMPLE_POS2NEG) {
            sent.mapPartitions(pairsSamplePos2Neg(_, samplingMode, curEpoch * numPartitions + pI, partitioner1, partitioner2, window,
              numPartitions))
          } else {
            assert(false)
            null
          }
          }.partitionBy(partitionerKey).values

        val newEmb = (cur.zipPartitions(embLR) { case (sIt, eItLR) =>
          val context = initContext(eItLR, expTable.value)

          ParItr.foreach(sIt, numThread)({ case (l, r) =>
            optimize(l, r, curLearningRate, context)
          })

          println("LOSS: " + context.loss.doubleValue() / context.lossn.longValue() + " (" + context.loss.doubleValue() + " / " + context.lossn.longValue() + ")")

          context.vocabL.keySet().iterator().asScala.map { e =>
            val k = e.asInstanceOf[ItemID]
            val v = context.vocabL(e)
            k -> (context.cnL(v), context.syn0.slice(v * vectorSize, (v + 1) * vectorSize), null.asInstanceOf[Array[Float]])
          } ++ context.vocabR.keySet().iterator().asScala.map { e =>
            val k = e.asInstanceOf[ItemID]
            val v = context.vocabR(e)
            k -> (0L, null.asInstanceOf[Array[Float]], context.syn1Neg.slice(v * vectorSize, (v + 1) * vectorSize))
          }
        })

        emb = newEmb
          .reduceByKey{case (x, y) => (x._1 + y._1, if (x._2 != null) x._2 else y._2, if (x._3 == null) y._3 else x._3)}
          .persist(intermediateRDDStorageLevel)
        cached += emb

        if (checkpointInterval > 0 && (checkpointIter + 1) % checkpointInterval == 0) {
          emb = checkpoint(emb, checkpointPath + "/" + curEpoch + "_" + (pI + 1))(sent.sparkContext)
          cached.foreach(_.unpersist())
          cached.clear()
        }
        checkpointIter += 1
      }
      if (checkpointInterval > 0) {
        emb = checkpoint(emb, checkpointPath + "/" + curEpoch)(sent.sparkContext)
      }
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
      .setSamplingMode(SkipGram.SamplingMode.WINDOW)
      .setLambda(0.0001)
      .setUseBias(true)
      .setPow(params.pow.getOrElse(0))
      .setIntermediateRDDStorageLevel(StorageLevel.DISK_ONLY_2)
      .setNumThread(numCores)
      .setNumPartitions(numExecutors)
      .setCheckpointPath(params.output)
      .setCheckpointInterval(params.checkpointInterval)
      .fit(data)

    println(System.currentTimeMillis() - start)
  }
}