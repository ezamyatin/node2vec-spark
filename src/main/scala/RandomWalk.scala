import util.SparkApp

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkContext}
import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.storage.StorageLevel
import scopt.OptionParser

case class RandomWalkParams(input: String = null,
                            output: String = null,
                            numWalks: Int = 0,
                            length: Int = 0,
                            p: Double = 1, q: Double = 1,
                            checkpointInterval: Int = 1)


object RandomWalk extends SparkApp[RandomWalkParams] {

  override def parser: Seq[String] => Option[RandomWalkParams] = new OptionParser[RandomWalkParams]("random-walk") {
    opt[String]("input").action((x, c) => c.copy(input = x))
    opt[String]("output").action((x, c) => c.copy(output = x))
    opt[Int]("numWalks").action((x, c) => c.copy(numWalks = x))
    opt[Int]("length").action((x, c) => c.copy(length = x))
    opt[Int]("checkpointInterval").action((x, c) => c.copy(checkpointInterval = x))
    opt[Double]("p").action((x, c) => c.copy(p = x))
    opt[Double]("q").action((x, c) => c.copy(q = x))
  }.parse(_, RandomWalkParams())

  def cacheAndCount[T](rdd: RDD[T]): RDD[T] = {
    val r = rdd.persist(StorageLevel.DISK_ONLY_2)
    r.count()
    r
  }

  def checkpoint(walks: RDD[(Array[Int], Array[Int])],
                 path: String)(implicit sc: SparkContext): RDD[(Array[Int], Array[Int])] = {
    val sqlc = new SQLContext(sc)
    import sqlc.implicits._
    explode(walks).toDF("walk", "prev_fr").write.mode(SaveMode.Overwrite).parquet(path)
    walks.unpersist()
    sqlc.read.parquet(path).as[(Array[Int], Array[Int])].rdd
  }

  def weightedRandom(weights: Array[Double], rnd: java.util.Random): Int = {
    val p = rnd.nextDouble()
    val sum = weights.sum
    var cur = 0.0
    var i = 0
    while (cur / sum <= p) {
      cur += weights(i)
      i += 1
    }
    i - 1
  }

  def explode(walk: RDD[(Array[Int], Array[Int])]): RDD[(Array[Int], Array[Int])] = {
    walk.mapPartitions(it => new Iterator[(Array[Int], Array[Int])] {
      var prev = 0
      var prevFr = Array.empty[Int]

      override def hasNext: Boolean = it.hasNext

      override def next(): (Array[Int], Array[Int]) = {
        val (w, fr) = it.next()
        if (fr == null) {
          assert(w.apply(w.length - 2) == prev)
        } else {
          prev = w.apply(w.length - 2)
          prevFr = fr
        }

        (w, prevFr)
      }
    })
  }

  def step(walks: RDD[(Array[Int], Array[Int])],
           graph: RDD[(Int, Array[Int])],
           p: Double, q: Double,
           inSeed: Int): RDD[(Array[Int], Array[Int])] = {
    explode(walks)
      .keyBy(_._1.last)
      .repartitionAndSortWithinPartitions(graph.partitioner.get)
      .zipPartitions(graph)((it1, it2) => new Iterator[(Array[Int], Array[Int])] {
        val it2b = it2.buffered
        var seed = inSeed
        val rnd = new java.util.Random(0)
        var prev = -1

        override def hasNext: Boolean = {
          it1.hasNext
        }

        override def next(): (Array[Int], Array[Int]) = {
          val cur = it1.next()
          seed = seed * 239017 + cur._1
          rnd.setSeed(seed)

          while (it2b.hasNext && it2b.head._1 < cur._1) {
            it2b.next()
          }
          if (it2b.hasNext && it2b.head._1 == cur._1) {
            val (_, (w, fr1)) = cur
            val fr2 = it2b.head._2
            val weights = Array.fill(fr2.length)(0.0)
            var i = 0
            var j = 0
            while (i < fr2.length) {
              while (j < fr1.length && fr1(j) < fr2(i)) {
                j += 1
              }

              if (j < fr1.length && fr1(j) == fr2(i)) {
                weights(i) = 1.0
              } else if (fr2(i) == w.apply(w.length - 2)) {
                weights(i) = 1.0 / p
              } else {
                weights(i) = 1.0 / q
              }

              i += 1
            }

            val k = weightedRandom(weights, rnd)
            val result = (w :+ fr2(k), if (w.last != prev) fr2 else null)
            prev = w.last
            result
          } else {
            null
          }
        }
      }.filter(_ != null))
  }

  override def run(params: RandomWalkParams)(implicit sc: SparkContext, sqlc: SQLContext): Unit = {
    import sqlc.implicits._

    val numExecutors = sc.getConf.get("spark.executor.instances").toInt
    val numCores = sc.getConf.get("spark.executor.cores").toInt
    val numPartitions = numExecutors * numCores

    val friends = cacheAndCount(sqlc.read.parquet(params.input)
        .as[(Int, Int)]
        .groupByKey(_._1)
        .mapGroups((u, arr) => u -> arr.map(_._2).toArray.sorted)
        .filter(_._2.length > 0)
        .rdd
        .repartitionAndSortWithinPartitions(new HashPartitioner(numPartitions)))

    var walks = friends.flatMap{case (u, fr) =>
      val rnd = new java.util.Random(u)
      Array.fill(params.numWalks)(Array(u, fr(rnd.nextInt(fr.length))) -> fr)
    }

    (0 until params.length).foreach {i =>
      val walks1 = cacheAndCount(step(walks, friends, params.p, params.q, i))
      walks.unpersist(true)
      walks = walks1

      if ((i + 1) % params.checkpointInterval == 0) {
        walks = checkpoint(walks, params.output + "/" + (i+1))
      }
    }

    checkpoint(walks, params.output + "/walks")
  }
}
