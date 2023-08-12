package utils.spark

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkUtils {

  val unitTestSparkConfigurations = Map[String, String](
    "spark.master" -> "local[4]",
    "spark.executor.memory" -> "4g",
    "spark.app.name" -> "testApp",
    "spark.sql.catalogImplementation" -> "in-memory",
    "spark.sql.shuffle.partitions" -> "1",
    "spark.sql.warehouse.dir" -> "target/spark-warehouse",
    "spark.hadoop.mapreduce.input.fileinputformat.input.dir.recursive" -> "true"
  )

  def getSparkConf: SparkConf = {
    new SparkConf().setAll(unitTestSparkConfigurations)
  }

  def createSparkSession(conf: SparkConf):SparkSession={
    SparkSession
      .builder()
      .config(conf)
      .getOrCreate()
  }
}
