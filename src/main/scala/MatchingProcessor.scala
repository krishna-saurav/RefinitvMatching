import org.apache.spark.SparkConf
import org.apache.spark.sql.{Encoders, SaveMode, SparkSession}

import scala.reflect.io.Directory

object MatchingProcessor extends App {
  val unitTestSparkConfigurations = Map[String, String](
    "spark.master" -> "local[4]",
    "spark.executor.memory" -> "4g",
    "spark.app.name" -> "testApp",
    "spark.sql.catalogImplementation" -> "in-memory",
    "spark.sql.shuffle.partitions" -> "1",
    "spark.sql.warehouse.dir" -> "target/spark-warehouse",
    "spark.hadoop.mapreduce.input.fileinputformat.input.dir.recursive" -> "true"
  )


  def prepareTestData()(implicit spark: SparkSession, database: String) = {

    val testDataWarehouseDir = Directory(s"target/spark-warehouse/${database}.db")
    if(testDataWarehouseDir.exists) testDataWarehouseDir.deleteRecursively()

    spark.sql(s"DROP DATABASE IF EXISTS ${database}")
    spark.sql(s"CREATE DATABASE IF NOT EXISTS ${database}")
  }

  private def registerKryoClasses(conf: SparkConf): SparkConf = {
    conf
      .registerKryoClasses(
        Array(
          classOf[FxOrder]
        )
      )
  }

  val conf = new SparkConf().setAll(unitTestSparkConfigurations)

  val confWithKryoRegisteredClasses = registerKryoClasses(conf)

  def getAnalogousOrderType(orderType: String): String ={
    if(orderType == "BUY") "SELL" else "BUY"
  }

  def matchFxOrders(order:FxOrder): Unit ={
    implicit val FxOrderEncoder = Encoders.product[FxOrder]

    val spark = SparkSession
                .builder()
                .config(confWithKryoRegisteredClasses)
                .getOrCreate()
    val targetOrderType = getAnalogousOrderType(order.order_type)
    val matchingOrder = spark.sql(
      s"""SELECT * FROM fx_trading.order_book where order_type='${targetOrderType}' and quantity='${order.quantity}' order by order_time asc limit 1 """
    ).as[FxOrder]

    val matchedOrderCount = matchingOrder.count()
    import spark.implicits._

    if(matchedOrderCount==1){
      matchingOrder.select("order_id".as("target_order_id"))
    }
    else{
      spark.catalog.tableExists("fx_trading", "order_book") match {
        case true => {
            List(order)
              .toDF()
              .select("")
              .write
              .mode(SaveMode.Append)
              .format("orc")
              .insertInto(s"fx_trading.order_book")
        }

        case false => {
          List(order)
            .toDF()
            .select("")
            .write
            .saveAsTable(s"fx_trading.order_book")
        }
      }
    }
  }
}
