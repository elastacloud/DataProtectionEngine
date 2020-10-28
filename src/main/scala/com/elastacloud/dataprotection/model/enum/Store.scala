package com.elastacloud.dataprotection.model.`enum`

import com.fasterxml.jackson.core.`type`.TypeReference

/**
 * Data Store Enumeration
 */
object Store extends Enumeration {
  type Store = Value
  val DBFS, LAKE, STORAGE, S3, LOCAL, GEN2 = Value
}

class Store extends TypeReference[Store.type]
