package com.elastacloud.dataprotection.columns

import org.apache.spark.sql.Column
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

import scala.util.hashing.MurmurHash3

/**
 * Object that registers multiple Hashing Functions as User Defined Functions
 */
object HashingFunctions {
  val murmurHash3: UserDefinedFunction = udf {
    MurmurHash3.stringHash _
  }
  val md5: Column => Column = org.apache.spark.sql.functions.md5
  val sha1: Column => Column = org.apache.spark.sql.functions.sha1
  val sha2: (Column, Int) => Column = org.apache.spark.sql.functions.sha2
  val hash: Seq[Column] => Column = org.apache.spark.sql.functions.hash
  val crc32: Column => Column = org.apache.spark.sql.functions.crc32

}
