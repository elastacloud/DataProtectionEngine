package com.elastacloud.dataprotection.model.protection

import com.elastacloud.dataprotection.model.common.DataSource

case class ProtectionPolicy(
                             version: String,
                             name: String,
                             source: DataSource,
                             destination: DataSource,
                             data: DataDescription
                           )
