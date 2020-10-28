package com.elastacloud.dataprotection

import com.elastacloud.dataprotection.IO.{DataReader, DataWriter}
import com.elastacloud.dataprotection.model.Bootstrap

object Main extends SharedSparkSession {

  spark.conf.set("spark.sql.files.ignoreCorruptFiles", "true")
  private val bootstrapper = new Bootstrap()

  def main(args: Array[String]): Unit = {

    val config = bootstrapper.getProtectionConfiguration(args(0))

    val data = DataReader.readData(config.source)

    val obfuscated = Obfuscation.run(data, config.protections)

    DataWriter.writeData(config.destination, obfuscated)
  }
}

