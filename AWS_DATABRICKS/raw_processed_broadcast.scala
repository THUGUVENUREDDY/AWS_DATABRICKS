// Databricks notebook source
val access_key = "AKIA22UAFDPYYDZBIXBL"
val secret_key = "hklNXmXCRwyqgN/5FjBvxYUzchUrZ3egN39VT4Zs"
val encoded_secret_key = secret_key.replace("/", "%2F")
val aws_bucket_name = "databrickprac"
val mount_name = "venu_mount"

dbutils.fs.mount(s"s3a://$access_key:$encoded_secret_key@$aws_bucket_name", s"/mnt/$mount_name")

// COMMAND ----------

//Read the data from S3 bucket
val read_df = spark.read
.option("infershcema","true")
.option("header","true")
.csv("dbfs:/mnt/venu_mount/venu/Raw_data/broadcast_right.csv")



display(read_df)

// COMMAND ----------

//modify the given file according to requirment
import org.apache.spark.sql.functions._



val trans_data = read_df.withColumn("broadcast_right_vod_type",
lower(col("broadcast_right_vod_type")))
.withColumn("load_date",current_date())
.withColumn("country_code",
when (col("broadcast_right_region") === "Denmark", "dk")
.when (col("broadcast_right_region") === "Baltics", "ba")
.when (col("broadcast_right_region") === "Bulgaria", "bg")
.when (col("broadcast_right_region") === "Estonia", "ee")
.when (col("broadcast_right_region") === "Finland", "fl")
.when (col("broadcast_right_region") === "Latvia", "lv")
.when (col("broadcast_right_region") === "Lithuania", "lt")
.when (col("broadcast_right_region") === "Nordic", "nd")
.when (col("broadcast_right_region") === "Norway", "no")
.when (col("broadcast_right_region") === "Russia", "ru")
.when (col("broadcast_right_region") === "Serbia", "rs")
.when (col("broadcast_right_region") === "Slovenia", "si")
.when (col("broadcast_right_region") === "Sweden", "se")
.when (col("broadcast_right_region") === "VNH Region Group","vnh")
.when (col("broadcast_right_region") === "Viasat History Region Group","vh")
.otherwise("nocode"))



display(trans_data)

// COMMAND ----------

trans_data.coalesce(1).write.option("header","true").csv("dbfs:/mnt/venu_mount/venu/Processed_data/processed_broadcast_Data/")