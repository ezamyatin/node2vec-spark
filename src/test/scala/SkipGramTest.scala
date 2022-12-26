import SkipGram.getPartition
import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel
import org.scalatest.FunSuite

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

class SkipGramTest extends FunSuite with SharedSparkContext {

  lazy val sqlc = new SQLContext(sc)

  override def conf: SparkConf = super.conf
    .set("spark.executor.instances", "5")
    .set("spark.executor.cores", "1")
    .set("spark.task.cpus", "1")

  test("SkipGramTest") {

    val numExecutors = sc.getConf.get("spark.executor.instances").toInt
    val numCores = sc.getConf.get("spark.executor.cores").toInt
    val rnd = new java.util.Random(0)
    val data = sc.parallelize((0 until 1000000).map{_ => val x = rnd.nextInt(1000); (0 until 10).map(i => (x + i)  % 1000).toArray})

    val model = new SkipGram()
      .setVectorSize(10)
      .setNegative(5)
      .setWindowSize(5)
      .setNumIterations(5)
      .setLearningRate(0.025)
      .setMinLearningRate(Some(0.00001))
      .setMinCount(0)
      .setSample(1e-4)
      .setIntermediateRDDStorageLevel(StorageLevel.DISK_ONLY_2)
      .setNumThread(numCores)
      .setNumPartitions(numExecutors)
      .fit(data)

    val fl = model.map(x => x._1 -> x._2._2).collectAsMap()

    def cos(a: Array[Float], b: Array[Float]): Double = {
      val n1 = Math.sqrt(a.map(Math.pow(_, 2)).sum)
      val n2 = Math.sqrt(b.map(Math.pow(_, 2)).sum)
      a.map(_/n1).zip(b.map(_/n2)).map(e => e._1 * e._2).sum
    }
    val recs = fl.map(e => e._1 -> cos(fl(10), e._2)).toArray.sortBy(-_._2).take(100)
    //println(recs.toSeq)
    assert((0 to 20).intersect(recs.map(_._1)).length == 21)
  }
}
