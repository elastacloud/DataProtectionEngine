package com.elastacloud.dataprotection.columns

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit

object Anonymizer {

  /**
   * Anonymize Columns replacing them with value "*****"
   *
   * @param value   value to replace columns with
   * @param columns column names to anonymize
   * @param df      data to anonymize columns from
   * @return data with listed columns replaced with value
   */
  def AnonymizeColumns(value: String, columns: String*)(df: DataFrame): DataFrame = {
    columns.foldLeft(df) { (data, col) => data.withColumn(col, lit(value)) }
  }
}
