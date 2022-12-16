# Databricks notebook source
scope="my_scope"
service_credential_key="my_sp"

storage_account=dbutils.secrets.get(scope=scope, key="storage_account")
application_id=dbutils.secrets.get(scope=scope, key="application_id")
directory_id=dbutils.secrets.get(scope=scope, key="directory_id")

service_credential = dbutils.secrets.get(scope=scope, key=service_credential_key)

spark.conf.set(f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account}.dfs.core.windows.net", f"{application_id}")
spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account}.dfs.core.windows.net", service_credential)
spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_account}.dfs.core.windows.net", f"https://login.microsoftonline.com/{directory_id}/oauth2/token")

# COMMAND ----------


