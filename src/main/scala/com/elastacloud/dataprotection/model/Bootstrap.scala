package com.elastacloud.dataprotection.model

import com.elastacloud.dataprotection.model.protection.ProtectionPolicy
import com.fasterxml.jackson.core.json.JsonReadFeature
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper

import scala.util.Try

class Bootstrap {
  // Create a json serializer
  private val jsonMapper = new ObjectMapper with ScalaObjectMapper
  jsonMapper.configure(JsonReadFeature.ALLOW_UNESCAPED_CONTROL_CHARS.mappedFeature(), true)
  jsonMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
  jsonMapper.registerModule(DefaultScalaModule)

  /**
   * Load Protection Configuration
   *
   * @param json JSON string to load
   * @return policy loaded from JSON
   */
  def getProtectionConfiguration(json: String): ProtectionPolicy = Try {
    jsonMapper.readValue[ProtectionPolicy](json)
  }.toOption match {
    case Some(json) => json
    case None => throw new NoSuchElementException("Error Parsing the protection policy")
  }
}
