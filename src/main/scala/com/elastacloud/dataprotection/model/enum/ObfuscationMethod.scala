package com.elastacloud.dataprotection.model.`enum`

import com.fasterxml.jackson.core.`type`.TypeReference

/**
 * Obfuscation Method Enumeration
 */
object ObfuscationMethod extends Enumeration {
  type Obfuscation = Value
  val PSEUDONYMIZE, ANONYMIZE, GENERALIZE = Value
}

class ObfuscationMethod extends TypeReference[ObfuscationMethod.type]