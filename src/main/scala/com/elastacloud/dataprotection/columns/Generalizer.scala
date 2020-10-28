package com.elastacloud.dataprotection.columns

import com.elastacloud.dataprotection.SharedSparkSession
import org.apache.spark.ml.feature.Bucketizer
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, max, min, udf}
import org.apache.spark.sql.types.DoubleType

class Generalizer(bucketSize: Double) extends SharedSparkSession {

  /**
   * Generalizes a [[DataFrame]] [[org.apache.spark.sql.Column]] based on bucket
   *
   * @param column column name to apply generalization against
   * @param df     data to apply generalization against
   * @return DataFrame with Column generalized
   */
  def generalize(column: String)(df: DataFrame): DataFrame = {

    val splits = getSplits(df, column)

    val bucketizer = new Bucketizer()
      .setInputCol(column)
      .setOutputCol(Generalizer.BUCKET_NAME)
      .setSplits(splits)

    val rangesBroadcast = spark.sparkContext.broadcast(splits)
    val getValue: UserDefinedFunction = udf((index: Int) => {
      (rangesBroadcast.value(index), rangesBroadcast.value(index + 1))
    })

    bucketizer.transform(df)
      .withColumn(column, getValue(col(Generalizer.BUCKET_NAME)))
      .drop(Generalizer.BUCKET_NAME)

  }

  /**
   * Get the splits for the column
   *
   * @param df     data to get splits from
   * @param column column name to get splits for
   * @return list of all splits
   */
  private def getSplits(df: DataFrame, column: String): Array[Double] = {

    val aggs = df.select(col(column).cast(DoubleType))
      .agg(
        max(column).alias(Generalizer.MAX_NAME),
        min(column).alias(Generalizer.MIN_NAME))

    val maxVal = aggs.first().getDouble(0)
    val minVal = aggs.first().getDouble(1)

    (minVal - bucketSize to maxVal + bucketSize by bucketSize).toArray
  }

  /**
   * Converts splits to ranges
   *
   * @param splits splits for the column
   * @return list of ranges the split covers
   */
  private def splitsToRanges(splits: Array[Double]): Array[String] = {
    var range: Array[String] = Array()
    for (i <- 0 to splits.length - 2) {
      range :+= s"${splits(i).toString}-${splits(i + 1).toString}"
    }

    range
  }


}

object Generalizer {
  private val MAX_NAME = "Max"
  private val MIN_NAME = "Min"
  private val BUCKET_NAME = "bucketedFeature"
}
