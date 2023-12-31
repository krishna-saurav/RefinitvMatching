package processors

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Encoders, SparkSession}
import schemas.FxOrder
import services.MatchingService.{matchFxOrders, prepareTestData}
import utils.cli.ProcessorUtils.parseInputParams
import utils.matching.MatchingUtils.registerMatchingEncodersWithSpark
import utils.spark.SparkUtils.{createSparkSession, getSparkConf}

object FxMatchingProcessor {


  def runMatchingProcessor(inputPath:String): Unit = {
    implicit val spark: SparkSession = createSparkSession(registerMatchingEncodersWithSpark(getSparkConf))
    implicit val FxOrderEncoder = Encoders.product[FxOrder].schema
    import spark.implicits._

    prepareTestData("fx_trading")

    val orderDataset = spark.read.format("csv").option("header","true").load(inputPath)

    orderDataset
      .select(
        $"order_id",
              $"user_name",
              $"order_time",
              $"order_type",
              $"quantity",
              col("price").cast(to = "int")
      )
      .as[FxOrder]
      .collectAsList()
      .forEach(order => {
        matchFxOrders(order)
      })
  }

  def main(args: Array[String]): Unit = {
    val cliArgs = parseInputParams(args)
    val inputPath = cliArgs("inputPath").asInstanceOf[String]
    runMatchingProcessor(inputPath)
  }
}
