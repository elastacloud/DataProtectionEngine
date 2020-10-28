package com.elastacloud.dataprotection.model.common

import com.elastacloud.dataprotection.model.enum.{AuthMode, FileFormat, Store}
import com.fasterxml.jackson.module.scala.JsonScalaEnumeration

/**
 * Description of a Data Source
 *
 * @param store          Store Type
 * @param name           name
 * @param location       location of data
 * @param fileFormat     format of file
 * @param authMode       store authentication mode
 * @param vaultName      name of Secret Scope
 * @param storageName    name of Storage Account
 * @param accountKeyName name of Storage Account Key in Secret Scope
 * @param clientKeyName  name of Client ID Key in Secret Scope
 * @param secretKeyName  name of Client Secret Key in Secret Scope
 * @param tenantKeyName  name of Tenant Key in Secret Scope
 *
 */
case class DataSource(
                       @JsonScalaEnumeration(classOf[Store]) store: Store.Store,
                       name: String,
                       location: String,
                       @JsonScalaEnumeration(classOf[FileFormat]) fileFormat: FileFormat.Format,
                       @JsonScalaEnumeration(classOf[AuthMode]) authMode: AuthMode.Auth,
                       vaultName: Option[String],
                       storageName: Option[String],
                       accountKeyName: Option[String],
                       clientKeyName: Option[String],
                       secretKeyName: Option[String],
                       tenantKeyName: Option[String]
                     )
