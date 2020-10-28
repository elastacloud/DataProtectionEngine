package com.elastacloud.dataprotection.model.enum

import com.fasterxml.jackson.core.`type`.TypeReference

/**
 * File Format Enumeration
 */
object FileFormat extends Enumeration {
  type Format = Value
  val JSON = Value("JSON")
  val CSV = Value("CSV")
  val PARQUET = Value("PARQUET")
  val DELTA = Value("DELTA")
}

class FileFormat extends TypeReference[FileFormat.type]