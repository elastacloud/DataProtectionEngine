package com.elastacloud.dataprotection.model.`enum`

import com.fasterxml.jackson.core.`type`.TypeReference

/**
 * Authentication Mode Enumeration
 */
object AuthMode extends Enumeration {
  type Auth = Value
  val KEY, SERVICE_PRINCIPLE, IAM = Value
}

class AuthMode extends TypeReference[AuthMode.type]