package com.elastacloud.dataprotection.IO

import com.elastacloud.dataprotection.SharedSparkSession
import com.elastacloud.dataprotection.model.common.DataSource
import com.elastacloud.dataprotection.model.enum.FileFormat._
import org.apache.spark.sql.DataFrame

object DataReader extends DataIO with SharedSparkSession {

  /**
   * Reads a specific [[DataSource]]
   *
   * @param source DataSource to read
   * @return [[DataFrame]] read in
   */
  def readData(source: DataSource): DataFrame = {
    readData(source, source.location)
  }

  /**
   * Reads a specific [[DataSource]] from a path
   *
   * @param source DataSource to read
   * @param path   to read
   * @return [[DataFrame]] read in
   */
  def readData(source: DataSource, path: String): DataFrame = {
    setConfig(source)

    source.fileFormat match {
      case JSON => readJSON(path, source)
      case CSV => readCSV(path, source)
      case PARQUET => readParquet(path)
      case DELTA => readDelta(path)
    }
  }

  /**
   * Reads JSON file
   *
   * @param source DataSource to read
   * @param path   path to read
   * @return [[DataFrame]] read in
   */
  private def readJSON(path: String, source: DataSource): DataFrame = {
    spark.read.json(path)
  }

  /**
   * Reads CSV file with FAILFAST mode for some stupid reason
   *
   * @param path   to read
   * @param source [[DataSource]] to read with additional CSV params
   * @return [[DataFrame]] read in
   */
  private def readCSV(path: String, source: DataSource): DataFrame = {
    spark.read.csv(path)
  }

  /**
   * Reads Parquet file
   *
   * @param path to read
   * @return [[DataFrame]] read in
   */
  private def readParquet(path: String) = {
    spark.read.parquet(path)

  }

  /**
   * Reads Delta Lake file
   *
   * @param path path to read
   * @return [[DataFrame]] read in
   */
  private def readDelta(path: String): DataFrame = {
    //noinspection ScalaCustomHdfsFormat
    spark.read.format("delta").load(path)
  }
}
