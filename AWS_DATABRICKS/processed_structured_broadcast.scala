// Databricks notebook source
//Read the broadcast data from S3 bucket
val broadcastdata = spark.read
.option("infershcema","true")
.option("header","true")
.csv("dbfs:/mnt/venu_mount/venu/Processed_data/processed_broadcast_Data/part-00000-tid-9193906158882690378-5495a1b4-0f41-471a-9fb2-0c090799cfe7-37-1-c000.csv")



display(broadcastdata)



// COMMAND ----------

//Read the stream data from S3 bucket
val streamdata = spark.read
.option("infershcema","true")
.option("header","true")
.csv("dbfs:/mnt/venu_mount/venu/Processed_data/processed_stream_Data/part-00000-tid-178326418125746124-022b68d6-079f-4469-986f-9db719be0dbe-27-1-c000.csv")


display(streamdata)


// COMMAND ----------

import org.apache.spark.sql.functions._



val transform_data = broadcastdata.join(streamdata, broadcastdata
.col("house_number") === streamdata.col("house_number") &&
broadcastdata.col("country_code") === streamdata.col("country_code"))
.drop(broadcastdata.col("house_number"))
.drop(broadcastdata.col("country_code"))
.drop(broadcastdata.col("dt"))
.select("dt",
"time",
"device_name",
"house_number",
"user_id",
"country_code",
"program_title",
"season",
"season_episode",
"genre",
"product_type",
"broadcast_right_start_date",
"broadcast_right_end_date")
.where(streamdata("product_type") === "tvod" || streamdata("product_type") === "est")
.withColumn("load_date", current_date())
.withColumn("year", year(col("load_date")))
.withColumn("month", month(col("load_date")))
.withColumn("day", dayofmonth(col("load_date")))



display(transform_data)

// COMMAND ----------

//write data local to S3 bucket using spacific path
transform_data.coalesce(1).write.partitionBy("year", "month", "day").option("header","true").csv("dbfs:/mnt/venu_mount/venu/Structured_data/structured_broadcast_data")

// COMMAND ----------

