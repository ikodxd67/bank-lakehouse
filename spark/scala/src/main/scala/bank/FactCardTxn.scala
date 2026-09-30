package bank

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

/** Выгрузка фактов по операциям для Greenplum.
  *
  * Берёт из ods операции, изменившиеся после прошлой выгрузки (по _batch_id),
  * обогащает картой, счётом, клиентом, ТСП и курсом на дату операции и пишет
  * Parquet в HDFS. Greenplum читает его через PXF.
  *
  * Соединение с ТСП перекошено: маркетплейс (merchant_id = 1) даёт четверть
  * операций. Стратегия соединения выбирается параметром, чтобы сравнить их замером.
  */
object FactCardTxn {

  sealed trait SkewStrategy
  object SkewStrategy {
    /** sort-merge join без защиты от перекоса: одна задача получает всю четверть */
    case object None extends SkewStrategy
    /** sort-merge join, перекошенную партицию AQE режет на части сам */
    case object Aqe extends SkewStrategy
    /** соль: горячие ключи раскладываются по N корзинам вручную */
    case object Salt extends SkewStrategy
    /** справочник целиком рассылается исполнителям, перемешивания нет вовсе */
    case object Broadcast extends SkewStrategy

    def parse(s: String): SkewStrategy = s.toLowerCase match {
      case "none"      => None
      case "aqe"       => Aqe
      case "salt"      => Salt
      case "broadcast" => Broadcast
      case other       => throw new IllegalArgumentException(s"неизвестная стратегия: $other")
    }
  }

  final case class Args(
      out: String = "hdfs://namenode:8020/data/export/fact_card_txn",
      strategy: SkewStrategy = SkewStrategy.Broadcast,
      saltBuckets: Int = 16,
      hotShare: Double = 0.01,
      full: Boolean = false,
      // замеры гоняют выгрузку целиком в отдельный каталог и не должны сдвигать водяной знак
      record: Boolean = true
  )

  def parseArgs(argv: Array[String]): Args =
    argv.sliding(2, 2).foldLeft(Args()) {
      case (a, Array("--out", v))          => a.copy(out = v)
      case (a, Array("--strategy", v))     => a.copy(strategy = SkewStrategy.parse(v))
      case (a, Array("--salt-buckets", v)) => a.copy(saltBuckets = v.toInt)
      case (a, Array("--hot-share", v))    => a.copy(hotShare = v.toDouble)
      case (a, Array("--full", v))         => a.copy(full = v.toBoolean)
      case (a, Array("--record", v))       => a.copy(record = v.toBoolean)
      case (_, other)                      => throw new IllegalArgumentException(s"не понял аргумент: ${other.mkString(" ")}")
    }

  /** Курс на каждый день и валюту: в выходные курс не публикуется, берётся последний известный. */
  def dailyFx(fx: DataFrame, dates: DataFrame): DataFrame = {
    val spark = fx.sparkSession
    import spark.implicits._
    val currencies = fx.select("currency").distinct()
    val grid = dates.crossJoin(currencies)
    val w = Window.partitionBy("currency").orderBy("rate_date").rowsBetween(Window.unboundedPreceding, 0)
    val filled = grid
      .join(fx.select($"rate_date", $"currency", $"rate_kzt"), Seq("rate_date", "currency"), "left")
      .withColumn("rate_kzt", last($"rate_kzt", ignoreNulls = true).over(w))
    // тенге к тенге — 1, в справочнике курсов его нет
    filled.unionByName(dates.withColumn("currency", lit("KZT")).withColumn("rate_kzt", lit(BigDecimal(1))))
  }

  /** Ключи, на которые в выборке приходится больше hotShare строк. */
  def hotKeys(df: DataFrame, key: String, hotShare: Double, fraction: Double = 0.01): Seq[Long] = {
    val sample = df.sample(withReplacement = false, fraction, seed = 42)
    val total = sample.count().toDouble
    if (total == 0) Seq.empty
    else
      sample.groupBy(key).count().where(col("count") > total * hotShare).collect().map(_.getLong(0)).toSeq
  }

  def joinMerchants(tx: DataFrame, merchants: DataFrame, strategy: SkewStrategy, hot: Seq[Long], buckets: Int): DataFrame =
    strategy match {
      case SkewStrategy.Broadcast => tx.join(broadcast(merchants), Seq("merchant_id"), "left")
      case SkewStrategy.None | SkewStrategy.Aqe => tx.join(merchants, Seq("merchant_id"), "left")
      case SkewStrategy.Salt =>
        val isHot: Column = if (hot.isEmpty) lit(false) else col("merchant_id").isin(hot: _*)
        // строка горячего ТСП уходит в одну из N корзин по хешу txn_id, остальные — в корзину 0
        val saltedTx = tx.withColumn("salt", when(isHot, pmod(hash(col("txn_id")), lit(buckets))).otherwise(lit(0)))
        // строку горячего ТСП размножаем на все N корзин, чтобы каждая нашла пару
        val saltedM = merchants
          .withColumn("salts", when(isHot, sequence(lit(0), lit(buckets - 1))).otherwise(array(lit(0))))
          .withColumn("salt", explode(col("salts")))
          .drop("salts")
        saltedTx.join(saltedM, Seq("merchant_id", "salt"), "left").drop("salt")
    }

  def buildFact(
      tx: DataFrame,
      cards: DataFrame,
      accounts: DataFrame,
      merchants: DataFrame,
      fx: DataFrame,
      strategy: SkewStrategy,
      hot: Seq[Long],
      buckets: Int
  ): DataFrame = {
    val withDate = tx.withColumn("txn_date", to_date(col("txn_ts")))
    val dates = withDate
      .agg(min("txn_date").as("lo"), max("txn_date").as("hi"))
      .select(explode(sequence(col("lo"), col("hi"))).as("rate_date"))

    // карты и счета — сотни тысяч строк, их рассылаем всегда
    val enriched = withDate
      .join(broadcast(cards.select("card_id", "account_id", "product")), Seq("card_id"), "left")
      .join(broadcast(accounts.select("account_id", "client_id")), Seq("account_id"), "left")

    val m = merchants.select(col("merchant_id"), col("mcc"), col("country").as("merchant_country"))
    joinMerchants(enriched, m, strategy, hot, buckets)
      .join(
        broadcast(dailyFx(fx, dates)).withColumnRenamed("rate_date", "txn_date"),
        Seq("txn_date", "currency"),
        "left"
      )
      .select(
        col("txn_id"),
        col("txn_ts"),
        col("txn_date"),
        col("card_id"),
        col("account_id"),
        col("client_id"),
        col("product"),
        col("merchant_id"),
        col("mcc"),
        col("merchant_country"),
        col("amount"),
        col("currency"),
        col("rate_kzt").cast("decimal(12,4)").as("fx_rate"),
        (col("amount") * col("rate_kzt")).cast("decimal(18,2)").as("amount_kzt"),
        col("status"),
        col("_is_deleted").as("is_deleted"),
        col("_version").as("version"),
        col("_batch_id").as("batch_id")
      )
  }

  def main(argv: Array[String]): Unit = {
    val args = parseArgs(argv)
    val spark = SparkSession.builder().appName(s"fact_card_txn_${args.strategy.toString.toLowerCase}").enableHiveSupport().getOrCreate()
    import spark.implicits._

    spark.sql(
      """CREATE TABLE IF NOT EXISTS ods._export_state (
        |  export_id bigint, since_batch bigint, max_batch bigint, rows bigint, strategy string, created_at timestamp
        |) USING iceberg""".stripMargin)

    args.strategy match {
      case SkewStrategy.None =>
        spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "false")
        spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
      case SkewStrategy.Aqe | SkewStrategy.Salt =>
        // ТСП всего 20 тыс., Spark сам разослал бы их; отключаем, чтобы сравнивать именно соединения с перемешиванием
        spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
      case SkewStrategy.Broadcast => ()
    }

    val since: Long =
      if (args.full) -1L
      else spark.sql("SELECT coalesce(max(max_batch), -1) FROM ods._export_state").as[Long].head()

    // Iceberg отсекает файлы по статистике min/max _batch_id: старые файлы не читаются
    val tx = spark.table("ods.card_transactions").where($"_batch_id" > since)
    val maxBatch = tx.agg(max("_batch_id")).as[Option[Long]].head()
    if (maxBatch.isEmpty) {
      println(s"новых операций после пакета $since нет")
      spark.stop()
      return
    }

    val hot = if (args.strategy == SkewStrategy.Salt) hotKeys(tx, "merchant_id", args.hotShare) else Seq.empty
    if (hot.nonEmpty) println(s"горячие ТСП: ${hot.mkString(", ")}")

    val fact = buildFact(
      tx,
      spark.table("ods.cards"),
      spark.table("ods.accounts"),
      spark.table("ods.merchants"),
      spark.table("ods.fx_rates"),
      args.strategy,
      hot,
      args.saltBuckets
    )

    val exportId = System.currentTimeMillis()
    val path = s"${args.out}/export_id=$exportId"
    val t0 = System.nanoTime()
    fact.write.mode("errorifexists").parquet(path)
    val seconds = (System.nanoTime() - t0) / 1e9
    val rows = spark.read.parquet(path).count()

    if (args.record) spark.sql(
      s"INSERT INTO ods._export_state VALUES ($exportId, $since, ${maxBatch.get}, $rows, '${args.strategy.toString.toLowerCase}', current_timestamp())"
    )
    println(f"выгрузка $exportId: $rows%,d строк за $seconds%.1f с (${args.strategy}), пакеты ($since, ${maxBatch.get}]")
    spark.stop()
  }
}
