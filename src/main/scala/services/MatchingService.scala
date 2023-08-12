package services

import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}
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

    /* Create empty tables for trade book and order book */
    List.empty[FxOrder]
      .toDF()
      .write
      .saveAsTable(s"fx_trading.order_book")
    List.empty[MatchingRecord]
      .toDF()
      .write
      .saveAsTable(s"fx_trading.trade_book")

    List.empty[FxOrder]
      .toDF()
      .write
      .saveAsTable(s"fx_trading.order_book_temp")
    List.empty[MatchingRecord]
      .toDF()
      .write
      .saveAsTable(s"fx_trading.trade_book_temp")
  }

  /* Save a particular order to Order book */
  def saveToOrderBook(order: FxOrder)(implicit spark: SparkSession): Unit = {
    import spark.implicits._
    val currentOrderBook = spark.sql(
      s"""SELECT * FROM fx_trading.order_book """
    )
    List(order)
      .toDF()
      .union(currentOrderBook)
      .write
      .mode(saveMode = SaveMode.Overwrite)
      .saveAsTable(s"fx_trading.order_book_temp")

    spark.sql(
      s"""SELECT * FROM fx_trading.order_book_temp """
    )
      .write
      .mode(saveMode = SaveMode.Overwrite)
      .saveAsTable(s"fx_trading.order_book")
  }

  /*Remove matched order from order book*/
  def updateOrderBook(matchingOrder: Dataset[FxOrder])(implicit spark: SparkSession): Unit = {

    val matchingOrderId = matchingOrder.collect()(0).order_id

    spark.sql(
      s"""SELECT * FROM fx_trading.order_book where order_id != ${matchingOrderId} """
    )
      .write
      .mode(saveMode = SaveMode.Overwrite)
      .saveAsTable(s"fx_trading.order_book_temp")

    spark.sql(
      s"""SELECT * FROM fx_trading.order_book_temp """
    )
      .write
      .mode(saveMode = SaveMode.Overwrite)
      .saveAsTable(s"fx_trading.order_book")
  }

  /* Find matching order from order book */
  def extractMatchingOrder(order:FxOrder)(implicit spark: SparkSession): Dataset[FxOrder] = {
    import spark.implicits._
    val pendingOrders = spark.sql(
      s"""SELECT * FROM fx_trading.order_book """
    ).as[FxOrder]

    val targetOrderType = getAnalogousOrderType(order.order_type)
    val targetPrice = order.order_type match  {

      case "BUY" => spark.sql(
        s"""select min(price) from fx_trading.order_book where
           |order_type='${targetOrderType}'
           |and quantity='${order.quantity}'
           |""".stripMargin)
        .collect()(0)
        .get(0)
        .asInstanceOf[Int]

      case "SELL" =>  spark.sql(
        s"""select max(price) from fx_trading.order_book where
           |order_type='${targetOrderType}'
           |and quantity='${order.quantity}'
           |""".stripMargin)
        .collect()(0)
        .get(0)
        .asInstanceOf[Int]
      case _ => throw new Exception("Invalid order type found")
    }
    findMatchingPriceOrder(targetPrice,targetOrderType,order.quantity)
  }

  /*Find first matching order for a given price, ordertype and quantity*/
  def findMatchingPriceOrder(price: Int,targetOrderType:String, quantity:String) (implicit spark: SparkSession): Dataset[FxOrder] = {
    import spark.implicits._
    spark.sql(
      s"""SELECT * FROM fx_trading.order_book where price=${price}
         |and order_type='${targetOrderType}'
         | and quantity='${quantity}'
         | order by order_time asc limit 1
         | """.stripMargin).as[FxOrder]
  }

  /*Update matching record to trade book*/
  def updateMatchingRecord(matchingOrder:Dataset[FxOrder], sourceOrderId:String)(implicit spark: SparkSession): Unit = {

    val currentOrderBook = spark.sql("SELECT * FROM fx_trading.trade_book")
    matchingOrder
      .select(
        col("order_id").as("target_order_id"),
        col("order_time"),
        col("quantity"),
        col("price")
      )
      .withColumn(
        "source_order_id",
        lit(sourceOrderId)
      )
      .union(currentOrderBook)
      .write
      .mode(SaveMode.Overwrite)
      .format("orc")
      .insertInto("fx_trading.trade_book_temp")

    spark.sql(
      s"""SELECT * FROM fx_trading.trade_book_temp """
    )
      .write
      .mode(saveMode = SaveMode.Overwrite)
      .saveAsTable(s"fx_trading.trade_book")
  }

  def matchFxOrders(order: FxOrder): Unit = {

    implicit val spark: SparkSession = createSparkSession(registerMatchingEncodersWithSpark(getSparkConf))
    spark.catalog.tableExists("fx_trading", "order_book") match {
      case true => {
        val matchingOrder = extractMatchingOrder(order)
        if (!matchingOrder.isEmpty) {
          updateMatchingRecord(matchingOrder,order.order_id)
          updateOrderBook(matchingOrder)
        }
        else
          saveToOrderBook(order)
      }
      case false =>
        saveToOrderBook(order)
    }
  }
}
