package utils.matching


import org.apache.spark.SparkConf
import schemas.{FxOrder, MatchingRecord}

object MatchingUtils {

  def registerMatchingEncodersWithSpark(conf:SparkConf): SparkConf = {
    conf
      .registerKryoClasses(
        Array(
          classOf[FxOrder],
          classOf[MatchingRecord]
        )
      )
  }
}
