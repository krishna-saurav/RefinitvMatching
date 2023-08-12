package matching.processors


import org.apache.spark.sql.SparkSession
import org.junit.jupiter.api.Test
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import services.MatchingService.{matchFxOrders, prepareTestData}
import schemas.FxOrder
import utils.matching.MatchingUtils.registerMatchingEncodersWithSpark
import utils.spark.SparkUtils.{createSparkSession, getSparkConf}

@RunWith(classOf[JUnitRunner])
class MatchingTestSuite  extends FunSuite with BeforeAndAfterAll  {
  implicit var spark: SparkSession = _

  def prepareTestEnv(): Unit = {
    val sparkConf = registerMatchingEncodersWithSpark(getSparkConf)
    spark = createSparkSession(sparkConf)
    prepareTestData("fx_trading")
  }

  @Test
  def updateOrderBook(): Unit = {
    prepareTestEnv()
    matchFxOrders(FxOrder("1","Ben","12345","SELL","4",2345))

    val orderBookCount = spark.sql("Select * from fx_trading.order_book").count()
    assert(orderBookCount==1)
  }

  def updateTradeBook(): Unit = {
    prepareTestEnv()
    matchFxOrders(FxOrder("1","Ben","12345","SELL","4",2345))
    matchFxOrders(FxOrder("2","Emma","4589","BUY","4",2345))

    val tradeBookCount = spark.sql("Select * from fx_trading.trade_book").count()
    assert(tradeBookCount==1)
  }
}
