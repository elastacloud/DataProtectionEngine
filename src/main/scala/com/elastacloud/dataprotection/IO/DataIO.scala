package com.elastacloud.dataprotection.IO

import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
import com.elastacloud.dataprotection.SharedSparkSession
import com.elastacloud.dataprotection.model.common.DataSource
import com.elastacloud.dataprotection.model.enum.{AuthMode, Store}

abstract class DataIO extends SharedSparkSession {
  /**
   * Set's Cloud Config for given [[DataSource]]
   *
   * @param source datasource to set config for
   */
  protected def setConfig(source: DataSource): Unit = {
    source.store match {
      case Store.LAKE => setAzureConfig(source)
      case Store.GEN2 => setAzureConfig(source)
      case Store.STORAGE => setAzureConfig(source)
      case Store.S3 => setAWSConfig()
    }
  }

  private def setAzureConfig(dataSource: DataSource): Unit = {
    dataSource.authMode match {
      case AuthMode.KEY => setKeyConfig(dataSource)
      case AuthMode.SERVICE_PRINCIPLE => setServicePrincipleConfig(dataSource)
    }
  }

  /**
   * Sets Azure Storage Gen1 or Gen2 Key config
   *
   * @param dataSource datasource to set config for
   */
  private def setKeyConfig(dataSource: DataSource): Unit = {
    val vaultName = dataSource.vaultName.getOrElse(throw new java.util.NoSuchElementException("Missing Vault Name"))
    val keyName = dataSource.accountKeyName.getOrElse(throw new java.util.NoSuchElementException("Missing Account Key Name"))
    val storageName = dataSource.storageName.getOrElse(throw new java.util.NoSuchElementException("Missing Storage Name"))

    if (dataSource.store == Store.GEN2) {
      spark.conf.set(s"fs.azure.account.key.$storageName.dfs.core.windows.net",
        dbutils.secrets.get(vaultName, keyName))
    }
    else {
      spark.conf.set(s"fs.azure.account.key.$storageName.blob.core.windows.net",
        dbutils.secrets.get(vaultName, keyName))
    }
  }

  /**
   * Sets Azure Storage Lake or Gen2 Service Principle config
   *
   * @param dataSource datasource to set config for
   */
  private def setServicePrincipleConfig(dataSource: DataSource): Unit = {
    val vaultName = dataSource.vaultName.getOrElse(throw new java.util.NoSuchElementException("Missing Vault Name"))
    val clientKey = dataSource.clientKeyName.getOrElse(throw new java.util.NoSuchElementException("Missing Client Key Name"))
    val secretKey = dataSource.secretKeyName.getOrElse(throw new java.util.NoSuchElementException("Missing Secret Key Name"))
    val tenantKey = dataSource.tenantKeyName.getOrElse(throw new java.util.NoSuchElementException("Missing Tenant Key Name"))

    if (dataSource.store == Store.LAKE) {
      spark.conf.set("dfs.adls.oauth2.access.token.provider.type", "ClientCredential")
      spark.conf.set("dfs.adls.oauth2.client.id", dbutils.secrets.get(vaultName, clientKey))
      spark.conf.set("dfs.adls.oauth2.credential", dbutils.secrets.get(vaultName, secretKey))
      spark.conf.set("dfs.adls.oauth2.refresh.url",
        s"https://login.microsoftonline.com/${dbutils.secrets.get(vaultName, tenantKey)}/oauth2/token")
    }

    else {
      val storageName = dataSource.storageName.getOrElse(throw new java.util.NoSuchElementException("Missing Storage Name"))

      spark.conf.set(s"fs.azure.account.oauth2.client.secret.$storageName.dfs.core.windows.net", dbutils.secrets.get(vaultName, secretKey))
      spark.conf.set(s"fs.azure.account.oauth2.client.endpoint.$storageName.dfs.core.windows.net", s"https://login.microsoftonline.com/${dbutils.secrets.get(vaultName, tenantKey)}/oauth2/token")
      spark.conf.set(s"fs.azure.account.auth.type.$storageName.dfs.core.windows.net", "OAuth")
      spark.conf.set(s"fs.azure.account.oauth.provider.type.$storageName.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
      spark.conf.set(s"fs.azure.account.oauth2.client.id.$storageName.dfs.core.windows.net", dbutils.secrets.get(vaultName, clientKey))
    }

  }

  /**
   * Sets AWS IAM config
   */
  private def setAWSConfig(): Unit = {
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.canned.acl", "BucketOwnerFullControl")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.acl.default", "BucketOwnerFullControl")
  }
}

