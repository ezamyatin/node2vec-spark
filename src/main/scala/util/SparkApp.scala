package util

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

abstract class SparkApp[T] {

  protected val LOG: Logger = {
    val result = Logger.getLogger(getClass)
    Logger.getRootLogger.setLevel(Level.ERROR)
    result.setLevel(Level.INFO)
    result
  }

  def conf: SparkConf = {
    new SparkConf()
      .setAppName(getClass.getName)
      .set("spark.yarn.executor.memoryOverhead", "3000")
      .set("spark.rdd.compress", "true")
      .set("spark.memory.fraction", "0.5")
      .set("spark.network.timeout", "300")
      .set("spark.kryoserializer.buffer", "8m")
      .set("spark.driver.maxResultSize", "4G")
  }

  def main(args: Array[String]): Unit = {
    parser(args).foreach {params =>
      implicit val sc = new SparkContext(conf)
      implicit val sqlc = new SQLContext(sc)
      run(params)
    }

  }

  def parser: Seq[String] => Option[T]

  def run(params: T)(implicit sc: SparkContext, sqlc: SQLContext): Unit
}
