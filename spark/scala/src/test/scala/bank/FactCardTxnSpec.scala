package bank

import bank.FactCardTxn.SkewStrategy
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.sql.{Date, Timestamp}

class FactCardTxnSpec extends AnyFunSuite with BeforeAndAfterAll {

  private lazy val spark: SparkSession = SparkSession
    .builder()
    .master("local[2]")
    .appName("fact-test")
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.sql.session.timeZone", "Asia/Almaty")
    .config("spark.ui.enabled", "false")
    .getOrCreate()

  import spark.implicits._

  override def afterAll(): Unit = spark.stop()

  private def fx: DataFrame = Seq(
    (Date.valueOf("2026-09-04"), "USD", BigDecimal("500.0000")),
    // 5 и 6 сентября — выходные, курса нет
    (Date.valueOf("2026-09-07"), "USD", BigDecimal("505.0000"))
  ).toDF("rate_date", "currency", "rate_kzt")

  private def tx: DataFrame = Seq(
    (1L, 10L, 1L, Timestamp.valueOf("2026-09-04 12:00:00"), BigDecimal("100.00"), "USD"),
    (2L, 10L, 1L, Timestamp.valueOf("2026-09-06 12:00:00"), BigDecimal("10.00"), "USD"),
    (3L, 11L, 1L, Timestamp.valueOf("2026-09-07 12:00:00"), BigDecimal("2.00"), "USD"),
    (4L, 11L, 2L, Timestamp.valueOf("2026-09-07 13:00:00"), BigDecimal("5000.00"), "KZT"),
    (5L, 11L, 3L, Timestamp.valueOf("2026-09-07 14:00:00"), BigDecimal("700.00"), "KZT")
  ).toDF("txn_id", "card_id", "merchant_id", "txn_ts", "amount", "currency")
    .withColumn("status", org.apache.spark.sql.functions.lit("posted"))
    .withColumn("_is_deleted", org.apache.spark.sql.functions.lit(false))
    .withColumn("_version", org.apache.spark.sql.functions.lit(2L))
    .withColumn("_batch_id", org.apache.spark.sql.functions.lit(0L))

  private def cards = Seq((10L, 100L, "classic"), (11L, 101L, "gold")).toDF("card_id", "account_id", "product")
  private def accounts = Seq((100L, 1000L), (101L, 1001L)).toDF("account_id", "client_id")
  private def merchants =
    Seq((1L, "5399", "KZ"), (2L, "5411", "KZ"), (3L, "5812", "KZ")).toDF("merchant_id", "mcc", "country")

  private def build(strategy: SkewStrategy, hot: Seq[Long] = Seq(1L)): Map[Long, (String, BigDecimal, Long)] =
    FactCardTxn
      .buildFact(tx, cards, accounts, merchants, fx, strategy, hot, buckets = 4)
      .select("txn_id", "mcc", "amount_kzt", "client_id")
      .as[(Long, String, BigDecimal, Long)]
      .collect()
      .map(r => r._1 -> (r._2, r._3, r._4))
      .toMap

  test("в выходные берётся последний известный курс") {
    val f = build(SkewStrategy.Broadcast)
    assert(f(1L)._2 == BigDecimal("50000.00"))
    assert(f(2L)._2 == BigDecimal("5000.00")) // воскресенье: курс пятницы
    assert(f(3L)._2 == BigDecimal("1010.00"))
  }

  test("тенге пересчитывается по курсу 1") {
    assert(build(SkewStrategy.Broadcast)(4L)._2 == BigDecimal("5000.00"))
  }

  test("все стратегии соединения дают один и тот же результат") {
    val expected = build(SkewStrategy.Broadcast)
    for (s <- Seq(SkewStrategy.None, SkewStrategy.Aqe, SkewStrategy.Salt)) {
      assert(build(s) == expected, s"стратегия $s")
    }
  }

  test("соль не размножает строки, даже если горячих ключей несколько") {
    val f = FactCardTxn.buildFact(tx, cards, accounts, merchants, fx, SkewStrategy.Salt, Seq(1L, 3L), buckets = 8)
    assert(f.count() == 5)
    assert(f.select("txn_id").distinct().count() == 5)
  }

  test("горячий ключ находится по выборке") {
    val skewed = (1 to 10000).map(i => (i.toLong, if (i % 4 == 0) 1L else (i % 997).toLong + 2)).toDF("txn_id", "merchant_id")
    assert(FactCardTxn.hotKeys(skewed, "merchant_id", hotShare = 0.05, fraction = 0.5) == Seq(1L))
  }

  test("разбор аргументов") {
    val a = FactCardTxn.parseArgs(Array("--strategy", "salt", "--salt-buckets", "32", "--record", "false"))
    assert(a.strategy == SkewStrategy.Salt && a.saltBuckets == 32 && !a.record)
    assertThrows[IllegalArgumentException](FactCardTxn.parseArgs(Array("--strategy", "magic")))
  }
}
