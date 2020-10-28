package com.elastacloud.dataprotection.IO

import java.io.FileNotFoundException

import com.elastacloud.dataprotection.SharedSparkSession
import com.elastacloud.dataprotection.model.common.DataSource
import org.apache.spark.sql.{DataFrame, SaveMode}

import scala.util.{Failure, Success, Try}

//noinspection ScalaCustomHdfsFormat
object Dictionary extends DataIO with SharedSparkSession {

  def readDictionary(source: DataSource): Try[DataFrame] = {
    setConfig(source)
    try {
      //create dataframe df
      val df = spark.read.format("delta").load(source.location)
      Success(df)
    } catch {
      case ex: FileNotFoundException =>
        println(s"Delta Table ${source.location} not found")
        Failure(ex)

      case unknown: Exception =>
        println(s"Unknown exception: $unknown")
        Failure(unknown)

    }
  }

  def updateDictionary(source: DataSource, dictionary: DataFrame): Unit = {
    dictionary.write.mode(SaveMode.Append).format("delta").save(source.location)
  }
}
