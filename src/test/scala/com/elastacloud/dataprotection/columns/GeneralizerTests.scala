package com.elastacloud.dataprotection.columns

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

class GeneralizerTests extends FlatSpec with Matchers with BeforeAndAfter with SharedSparkContext {
  lazy val spark: SparkSession = SparkSession.builder.getOrCreate()

  import spark.implicits._

  var source: DataFrame = _

  before {
    source = Seq(
      ("Andy", 21),
      ("Laura", 22),
      ("Matt", 24),
      ("Sarah", 50)
    ).toDF(GeneralizerTests.NAME_COLUMN, GeneralizerTests.AGE_COLUMN)
  }

  "generalize" should "result in the same shape as the source" in {
    val generalizer: Generalizer = new Generalizer(GeneralizerTests.SPLIT)
    val result = source.transform(generalizer.generalize(GeneralizerTests.AGE_COLUMN))

    source.columns.length shouldEqual result.columns.length
    source.count shouldEqual result.count
  }

  it should "change Age to String Type" in {
    val generalizer: Generalizer = new Generalizer(GeneralizerTests.SPLIT)
    val result = source.transform(generalizer.generalize(GeneralizerTests.AGE_COLUMN))

    result.schema.filter(_.name == GeneralizerTests.AGE_COLUMN).head.dataType shouldBe
      StructType(
        List(
          StructField("_1", DoubleType, nullable = false),
          StructField("_2", DoubleType, nullable = false)
        )
      )
  }

  it should "contain three values in 21-26 range" in {
    val generalizer: Generalizer = new Generalizer(GeneralizerTests.SPLIT)
    val result = source.transform(generalizer.generalize(GeneralizerTests.AGE_COLUMN))

    result.filter(col(s"${GeneralizerTests.AGE_COLUMN}._1") === 21.0).count shouldEqual 3
    result.filter(col(s"${GeneralizerTests.AGE_COLUMN}._2") === 26.0).count shouldEqual 3
  }
}

object GeneralizerTests {
  private val NAME_COLUMN = "Name"
  private val AGE_COLUMN = "Age"
  private val SPLIT = 5
}
