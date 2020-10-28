package com.elastacloud.dataprotection.model.protection

import com.elastacloud.dataprotection.model.`enum`.ObfuscationMethod
import com.elastacloud.dataprotection.model.common.DataSource
import com.fasterxml.jackson.module.scala.JsonScalaEnumeration

case class ProtectionDescription(
                                  @JsonScalaEnumeration(classOf[ObfuscationMethod]) obfuscation: ObfuscationMethod.Obfuscation,
                                  columns: List[String],
                                  dictionary: Option[DataSource],
                                  split: Option[Number],
                                  value: Option[String]
                                )
