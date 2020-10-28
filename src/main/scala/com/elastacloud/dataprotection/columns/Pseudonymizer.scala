package com.elastacloud.dataprotection.columns

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, concat, udf}

import scala.util.Random

object Pseudonymizer {

  private val TOKEN_COLUMN = "Token"
  private val KEY_COLUMN = "Key"

  def PseudonymizeColumn(df: DataFrame, dictionary: DataFrame, column: String): (DataFrame, DataFrame) = {
    val randomUDF = udf(() => Random.alphanumeric.take(10).mkString)

    val data = df.withColumnRenamed(column, KEY_COLUMN)
    val keyCol = col(KEY_COLUMN)
    val tokenCol = col(TOKEN_COLUMN)
    val keys = data.select(keyCol).distinct.filter(keyCol.isNotNull).cache

    val hasTokens = keys.join(dictionary, Seq(KEY_COLUMN))
    val newTokens = keys.join(dictionary, Seq(KEY_COLUMN), "leftanti").withColumn(TOKEN_COLUMN, randomUDF())
    val allTokens = newTokens.union(hasTokens)

    val pseudonmyized = data.join(allTokens, Seq(KEY_COLUMN), "left")
      .withColumn(column, HashingFunctions.md5(concat(tokenCol, keyCol)))
      .drop(TOKEN_COLUMN, KEY_COLUMN)
      .select(df.columns.head, df.columns.tail: _*)

    (pseudonmyized, newTokens)
  }
}
