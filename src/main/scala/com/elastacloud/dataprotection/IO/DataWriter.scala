package com.elastacloud.dataprotection.IO

import com.elastacloud.dataprotection.model.`enum`.FileFormat
import com.elastacloud.dataprotection.model.common.DataSource
import org.apache.spark.sql.{DataFrame, SaveMode}

object DataWriter extends DataIO {

  /**
   * Write a [[DataFrame]] to [[DataSource]]
   *
   * @param source DataSource to write
   * @param data   data to write
   */
  def writeData(source: DataSource, data: DataFrame): Unit = {
    writeData(source, data, source.location)
  }

  /**
   * Write a [[DataFrame]] to [[DataSource]]
   *
   * @param source DataSource to write
   * @param data   data to write
   * @param path   path to write data
   */
  def writeData(source: DataSource, data: DataFrame, path: String): Unit = {
    setConfig(source)

    source.fileFormat match {
      case FileFormat.JSON => writeJSON(path, data, source)
      case FileFormat.CSV => writeCSV(path, data, source)
      case FileFormat.PARQUET => writeParquet(path, data, source)
      case FileFormat.DELTA => writeDelta(path, data)
    }
  }

  /**
   * Write JSON File
   *
   * @param path path to write data
   * @param data data to write
   */
  private def writeJSON(path: String, data: DataFrame, source: DataSource): Unit = {
    data.write.mode(SaveMode.Overwrite).json(path)
  }

  /**
   * Write CSV File
   *
   * @param path   path to write data
   * @param data   data to write
   * @param source [[DataSource]] to write with additional CSV params
   */
  private def writeCSV(path: String, data: DataFrame, source: DataSource): Unit = {
    data.write.mode(SaveMode.Overwrite).csv(path)
  }

  /**
   * Write Parquet file
   *
   * @param path path to write data
   * @param data data to write
   */
  private def writeParquet(path: String, data: DataFrame, source: DataSource): Unit = {
    data.write.mode(SaveMode.Overwrite).parquet(path)
  }

  /**
   * Write Delta Lake file
   *
   * @param path path to write data
   * @param data data to write
   */
  private def writeDelta(path: String, data: DataFrame): Unit = {
    //noinspection ScalaCustomHdfsFormat
    data.write.mode(SaveMode.Overwrite).format("delta").save(path)
  }
}
