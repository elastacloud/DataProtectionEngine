package com.elastacloud.dataprotection.columns

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.sql.functions.{col, lit, when}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

class HashingFunctionTest extends FlatSpec with Matchers with BeforeAndAfter with SharedSparkContext {

  lazy val spark: SparkSession = SparkSession.builder.getOrCreate()

  import spark.implicits._

  var source: DataFrame = _

  before {
    source = Seq(
      ("Andy", "M"),
      ("Matt", "M"),
      ("Sarah", "F")
    ).toDF(HashingFunctionTest.NAME_COL, "Gender")
  }

  "sha 1 HashingFunctions" should "add a new column with a different value" in {
    val result = source.withColumn(HashingFunctionTest.HASH_COL, HashingFunctions.sha1(col(HashingFunctionTest.NAME_COL)))
      .withColumn("same_value", when(col(HashingFunctionTest.NAME_COL) === col(HashingFunctionTest.HASH_COL), lit(true)).otherwise(lit(false)))

    result.columns.length shouldEqual source.columns.length + 2
    result.filter(col(HashingFunctionTest.HASH_COL) === true).count shouldBe 0
  }

  "sha 256 HashingFunctions" should "add a new column with a different value" in {
    val result = source.withColumn(HashingFunctionTest.HASH_COL, HashingFunctions.sha2(col(HashingFunctionTest.NAME_COL), 0))
      .withColumn("same_value", when(col(HashingFunctionTest.NAME_COL) === col(HashingFunctionTest.HASH_COL), lit(true)).otherwise(lit(false)))

    result.columns.length shouldEqual source.columns.length + 2
    result.filter(col(HashingFunctionTest.HASH_COL) === true).count shouldBe 0
  }

  "md5 HashingFunctions" should "add a new column with a different value" in {
    val result = source.withColumn(HashingFunctionTest.HASH_COL, HashingFunctions.md5(col(HashingFunctionTest.NAME_COL)))
      .withColumn("same_value", when(col(HashingFunctionTest.NAME_COL) === col(HashingFunctionTest.HASH_COL), lit(true)).otherwise(lit(false)))

    result.columns.length shouldEqual source.columns.length + 2
    result.filter(col(HashingFunctionTest.HASH_COL) === true).count shouldBe 0
  }

  "crc32 HashingFunctions" should "add a new column with a different value" in {
    val result = source.withColumn(HashingFunctionTest.HASH_COL, HashingFunctions.crc32(col(HashingFunctionTest.NAME_COL)))
      .withColumn("same_value", when(col(HashingFunctionTest.NAME_COL) === col(HashingFunctionTest.HASH_COL), lit(true)).otherwise(lit(false)))

    result.columns.length shouldEqual source.columns.length + 2
    result.filter(col(HashingFunctionTest.HASH_COL) === true).count shouldBe 0
  }

  "murmur Hash 3 HashingFunctions" should "add a new column with a different value" in {
    val result = source.withColumn(HashingFunctionTest.HASH_COL, HashingFunctions.murmurHash3(col(HashingFunctionTest.NAME_COL)))
      .withColumn("same_value", when(col(HashingFunctionTest.NAME_COL) === col(HashingFunctionTest.HASH_COL), lit(true)).otherwise(lit(false)))

    result.columns.length shouldEqual source.columns.length + 2
    result.filter(col(HashingFunctionTest.HASH_COL) === true).count shouldBe 0
  }
}

object HashingFunctionTest {
  private val NAME_COL = "name"
  private val HASH_COL = "hash"
}

