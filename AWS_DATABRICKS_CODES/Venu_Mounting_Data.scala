// Databricks notebook source
val access_key = "AKIA22UAFDPYYDZBIXBL"
val secret_key = "hklNXmXCRwyqgN/5FjBvxYUzchUrZ3egN39VT4Zs"
val encoded_secret_key = secret_key.replace("/", "%2F")
val aws_bucket_name = "databrickprac"
val mount_name = "venu_mount"

dbutils.fs.mount(s"s3a://$access_key:$encoded_secret_key@$aws_bucket_name", s"/mnt/$mount_name")

// COMMAND ----------

// MAGIC %fs mounts

// COMMAND ----------

display(dbutils.fs.ls("/mnt/venu_mount"))

// COMMAND ----------

