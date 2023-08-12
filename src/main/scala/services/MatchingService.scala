package services

import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{SaveMode, SparkSession}
import schemas.{FxOrder, MatchingRecord}
import utils.matching.MatchingUtils.registerMatchingEncodersWithSpark
import utils.spark.SparkUtils.{createSparkSession, getSparkConf}

import scala.reflect.io.Directory

object MatchingService {

  def getAnalogousOrderType(orderType: String): String = {
    if (orderType == "BUY") "SELL" else "BUY"
  }

  def prepareTestData(database: String)(implicit spark: SparkSession): Unit = {

    import spark.implicits._
    val testDataWarehouseDir = Directory(s"target/spark-warehouse/${database}.db")
    if (testDataWarehouseDir.exists) testDataWarehouseDir.deleteRecursively()

    spark.sql(s"DROP DATABASE IF EXISTS ${database}")
    spark.sql(s"CREATE DATABASE IF NOT EXISTS ${database}")

    List(FxOrder("111000111", "xxx", "12345", "xx", "0", "2345"))
      .toDF()
      .write
      .saveAsTable(s"fx_trading.order_book")
    List(MatchingRecord("111000111", "111000222", "12345", "0", "2345"))
      .toDF()
      .write
      .saveAsTable(s"fx_trading.trade_book")
  }

  def matchFxOrders(order: FxOrder): Unit = {
    val spark = createSparkSession(registerMatchingEncodersWithSpark(getSparkConf))
    import spark.implicits._

    val targetOrderType = getAnalogousOrderType(order.order_type)

    spark.catalog.tableExists("fx_trading", "order_book") match {
      case true => {
        val matchingOrder = spark.sql(
          s"""SELECT * FROM fx_trading.order_book where order_type='${targetOrderType}' and quantity='${order.quantity}' order by order_time asc limit 1 """
        ).as[FxOrder]

        val matchedOrderCount = matchingOrder.count()


        if (matchedOrderCount == 1) {
          matchingOrder
            .select(
              $"order_id".as("target_order_id"),
              $"order_time",
              $"quantity",
              $"price"
            )
            .withColumn(
              "source_order_id",
              lit(order.order_id)
            )
            .write
            .mode(SaveMode.Append)
            .format("orc")
            .insertInto("fx_trading.trade_book")

          spark.sql(s"""DELETE FROM fx_trading.order_book WHERE order_id='${matchingOrder}'""")
        }
        else {
          List(order)
            .toDF()
            .write
            .mode(SaveMode.Append)
            .format("orc")
            .insertInto(s"fx_trading.order_book")

        }
      }
      case false => {
        List(order)
          .toDF()
          .write
          .saveAsTable(s"fx_trading.order_book")
      }
    }
  }
}
