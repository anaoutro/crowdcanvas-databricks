# Databricks notebook source
# MAGIC %md
# MAGIC # CrowdCanvas — Watermarked event-time windows
# MAGIC This separate example consumes the **curated unique valid** JSON fixtures, not the raw corrupt CSV.
# MAGIC Historical analysis and streaming are deliberately different products. The former corrects all valid late history;
# MAGIC streaming bounds state, may omit old arrivals, and emits only finalized windows in append mode.
# MAGIC No promise is made that its output equals the historical reference results.
# MAGIC Keep the checkpoint when restarting. Use a new table and checkpoint for a fresh experiment.

# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import StructType,StructField,StringType,LongType,DoubleType
import re
dbutils.widgets.text("catalog","main")
dbutils.widgets.text("schema","crowdcanvas")
dbutils.widgets.text("stream_path","/Volumes/main/crowdcanvas/input/stream_input")
dbutils.widgets.text("checkpoint_path","/Volumes/main/crowdcanvas/checkpoints/flow_v1")
catalog=dbutils.widgets.get("catalog");schema_name=dbutils.widgets.get("schema")
stream_path=dbutils.widgets.get("stream_path");checkpoint_path=dbutils.widgets.get("checkpoint_path")
assert re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*",catalog)
assert re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*",schema_name)
assert all(p.startswith("/Volumes/") and ".." not in p for p in [stream_path,checkpoint_path])
assert stream_path!=checkpoint_path
spark.conf.set("spark.sql.session.timeZone","UTC")
prefix=f"{catalog}.{schema_name}"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {prefix}")
schema=StructType([StructField("event_id",StringType()),StructField("zone",StringType()),
    StructField("event_time",StringType()),StructField("arrival_time",StringType()),
    StructField("occupancy",LongType()),StructField("queue_wait_minutes",DoubleType())])
zones=spark.createDataFrame([("MAIN",1000),("HARBOR",500),("GROVE",700),("FOOD",300)],["zone","capacity"])
source=(spark.readStream.schema(schema).option("maxFilesPerTrigger",1).json(stream_path)
    .withColumn("observed_at",F.to_timestamp("event_time"))
    .withColumn("arrived_at",F.to_timestamp("arrival_time"))
    .join(F.broadcast(zones),"zone","inner")
    .filter("observed_at IS NOT NULL AND arrived_at >= observed_at AND occupancy BETWEEN 0 AND capacity AND queue_wait_minutes BETWEEN 0 AND 120")
    .withColumn("late_arrival",(F.unix_timestamp("arrived_at")-F.unix_timestamp("observed_at"))>600))
# Input uniqueness is an upstream contract for this example. Do not add stateful deduplication
# blindly in front of aggregation; evaluate supported stateful operator combinations first.
windows=(source.withWatermark("observed_at","10 minutes")
    .groupBy(F.window("observed_at","5 minutes"),"zone","capacity")
    .agg(F.count("*").alias("samples"),F.max("occupancy").alias("peak_occupancy"),
         F.round(F.avg("queue_wait_minutes"),2).alias("mean_wait_minutes")))
output=windows.select("zone","capacity",F.col("window.start").alias("window_start"),F.col("window.end").alias("window_end"),
    "samples","peak_occupancy","mean_wait_minutes")
query=(output.writeStream.format("delta").outputMode("append").option("checkpointLocation",checkpoint_path)
    .trigger(availableNow=True).toTable(f"{prefix}.gold_stream_windows"))
query.awaitTermination()
display(spark.table(f"{prefix}.gold_stream_windows").orderBy("window_start","zone"))
# The final windows may remain unfinalized without newer event times advancing the watermark.
# File processing order and micro-batches affect which older records are beyond the watermark.
