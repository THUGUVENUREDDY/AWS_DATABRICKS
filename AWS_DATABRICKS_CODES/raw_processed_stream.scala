// Databricks notebook source
val access_key = "AKIA22UAFDPYYDZBIXBL"
val secret_key = "hklNXmXCRwyqgN/5FjBvxYUzchUrZ3egN39VT4Zs"
val encoded_secret_key = secret_key.replace("/", "%2F")
val aws_bucket_name = "databrickprac"
val mount_name = "venu_mount"

dbutils.fs.mount(s"s3a://$access_key:$encoded_secret_key@$aws_bucket_name", s"/mnt/$mount_name")

// COMMAND ----------

//Read the data from S3 bucket
val readdf = spark.read
.option("inferschema","true")
.option("header","true")
.option("sep",";")
.csv("dbfs:/mnt/venu_mount/venu/Raw_data/started_streams.csv")
readdf.show(7)

// COMMAND ----------

//Read the data from S3 bucket
val read_data = spark.read
  .option("header", true)
  .option("inferschema", true)
  .option("delimiter", ";")
  .csv("dbfs:/mnt/venu_mount/venu/Raw_data/started_streams.csv")
display(read_data)

// COMMAND ----------

//modify the given file according to requirment
import org.apache.spark.sql.functions._



val trans_data = readdf
.withColumn("load_date",current_date())



display(trans_data)

// COMMAND ----------

trans_data.coalesce(1).write.option("header","true").csv("dbfs:/mnt/venu_mount/venu/Processed_data/processed_stream_Data/")

// COMMAND ----------

