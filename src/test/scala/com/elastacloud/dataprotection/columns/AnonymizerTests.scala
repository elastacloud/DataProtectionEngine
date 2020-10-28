package com.elastacloud.dataprotection.columns

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

class AnonymizerTests extends FlatSpec with Matchers with BeforeAndAfter with SharedSparkContext {
  lazy val spark: SparkSession = SparkSession.builder.getOrCreate()

  import spark.implicits._

  var source: DataFrame = _
  before {
    source = Seq(
      ("Andy", 10, 25),
      ("Andy", 20, 35),
      ("Matt", 30, 35),
      ("Sarah", 40, 45)
    ).toDF(AnonymizerTests.NAME_COL, "Salary", AnonymizerTests.AGE_COL)
  }

  "AnonymizeColumns" should "replace name and age with whitespace's" in {
    val defaultReplace = ""
    val columns = Seq(AnonymizerTests.NAME_COL, AnonymizerTests.AGE_COL)

    val result = source.transform(Anonymizer.AnonymizeColumns("", columns: _*))

    result.filter(col(AnonymizerTests.NAME_COL) =!= defaultReplace).count shouldEqual 0
    result.filter(col(AnonymizerTests.AGE_COL) =!= defaultReplace).count shouldEqual 0
  }

  it should "be the same shape as the source" in {
    val columns = Seq(AnonymizerTests.NAME_COL, AnonymizerTests.AGE_COL)

    val result = source.transform(Anonymizer.AnonymizeColumns("", columns: _*))

    result.count shouldEqual source.count
    result.columns.length shouldEqual source.columns.length
  }

  "AnonymizeColumnsWithValue" should "replace name and age with -'s" in {
    val defaultReplace = "-"
    val columns = Seq(AnonymizerTests.NAME_COL, AnonymizerTests.AGE_COL)

    val result = source.transform(Anonymizer.AnonymizeColumns(defaultReplace, columns: _*))

    result.filter(col(AnonymizerTests.NAME_COL) =!= defaultReplace).count shouldEqual 0
    result.filter(col(AnonymizerTests.AGE_COL) =!= defaultReplace).count shouldEqual 0
  }

  it should "be the same shape as the source" in {
    val defaultReplace = "-"
    val columns = Seq(AnonymizerTests.NAME_COL, AnonymizerTests.AGE_COL)

    val result = source.transform(Anonymizer.AnonymizeColumns(defaultReplace, columns: _*))

    result.count shouldEqual source.count
    result.columns.length shouldEqual source.columns.length
  }

  it should "replace name with -'s and age with x's" in {
    val replaceName = "-"
    val replaceAge = "x"

    val result = source
      .transform(Anonymizer.AnonymizeColumns(replaceName, AnonymizerTests.NAME_COL))
      .transform(Anonymizer.AnonymizeColumns(replaceAge, AnonymizerTests.AGE_COL))

    result.filter(col(AnonymizerTests.NAME_COL) =!= replaceName).count shouldEqual 0
    result.filter(col(AnonymizerTests.AGE_COL) =!= replaceAge).count shouldEqual 0
  }
}

object AnonymizerTests {
  private val NAME_COL = "Name"
  private val AGE_COL = "AGE"
}
