// Databricks notebook source
import org.apache.spark.sql.functions._
val read_data = spark.read
  .option("header", true)
  .option("inferschema", true)
  .csv("dbfs:/mnt/venu_mount/venu/Processed_data/processed_stream_Data/part-00000-tid-178326418125746124-022b68d6-079f-4469-986f-9db719be0dbe-27-1-c000.csv")
display(read_data)

// COMMAND ----------

val trans_Stream = read_data.select("dt","device_name","product_type","user_id","program_title","country_code")
                                .groupBy("dt","device_name","product_type","program_title","country_code")
                                .agg(countDistinct("user_id") as "unique_users",count("program_title") as "content_count")
                                .withColumn("load_date", current_date())
                                .withColumn("year", year(col("load_date")))
                                .withColumn("month", month(col("load_date")))
                                .withColumn("day", dayofmonth(col("load_date")))
                                .sort(col("unique_users").desc)
display(trans_Stream)

// COMMAND ----------

trans_Stream.coalesce(1).write.option("header","true")
.partitionBy("year", "month", "day")
.csv("dbfs:/mnt/venu_mount/venu/Structured_data/Structured_data_stream")

// COMMAND ----------

