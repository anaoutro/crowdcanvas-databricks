# Databricks notebook source
# MAGIC %md
# MAGIC # CrowdCanvas — Corrected historical festival flow
# MAGIC Synthetic, anonymous zone snapshots. Historical analysis retains all valid late arrivals.
# MAGIC Bronze incrementally merges payloads. Silver and Gold are rebuilt to revoke conflicted identities.
# MAGIC Upload data/festival_events.csv to a Unity Catalog volume before running.

# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType
from delta.tables import DeltaTable
import re
dbutils.widgets.text("catalog","main")
dbutils.widgets.text("schema","crowdcanvas")
dbutils.widgets.text("input_path","/Volumes/main/crowdcanvas/input/festival_events.csv")
catalog=dbutils.widgets.get("catalog"); schema_name=dbutils.widgets.get("schema")
input_path=dbutils.widgets.get("input_path")
assert re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*",catalog)
assert re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*",schema_name)
assert input_path.startswith("/Volumes/") and input_path.endswith(".csv") and ".." not in input_path
prefix=f"{catalog}.{schema_name}"
spark.conf.set("spark.sql.session.timeZone","UTC")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {prefix}")
columns=["event_id","zone","event_time","arrival_time","occupancy","queue_wait_minutes"]
zones=spark.createDataFrame([("MAIN",1000),("HARBOR",500),("GROVE",700),("FOOD",300)],["zone","capacity"])
schema=StructType([StructField(c,StringType(),True) for c in columns])

# COMMAND ----------
raw=spark.read.option("header",True).option("mode","FAILFAST").schema(schema).csv(input_path)
incoming=(raw.withColumn("payload_hash",F.sha2(F.to_json(F.struct(*[F.col(c) for c in columns])),256))
    .withColumn("source_file",F.lit(input_path)).withColumn("ingested_at",F.current_timestamp()).dropDuplicates(["payload_hash"]))
bronze_table=f"{prefix}.bronze_snapshots"
if not spark.catalog.tableExists(bronze_table):
    incoming.write.format("delta").mode("append").saveAsTable(bronze_table)
else:
    (DeltaTable.forName(spark,bronze_table).alias("t").merge(incoming.alias("s"),"t.payload_hash=s.payload_hash")
     .whenNotMatchedInsertAll().execute())

# COMMAND ----------
bronze=spark.table(bronze_table)
conflicted_ids=(bronze.filter(F.col("event_id").isNotNull() & (F.trim("event_id")!=""))
    .groupBy("event_id").agg(F.countDistinct("payload_hash").alias("versions"))
    .filter("versions > 1").select("event_id").withColumn("identity_conflict",F.lit(True)))
typed=(bronze.join(F.broadcast(zones),"zone","left")
    .join(conflicted_ids,"event_id","left")
    .withColumn("observed_at",F.expr("try_cast(event_time AS TIMESTAMP)"))
    .withColumn("arrived_at",F.expr("try_cast(arrival_time AS TIMESTAMP)"))
    .withColumn("occupancy_value",F.expr("try_cast(occupancy AS DECIMAL(10,2))"))
    .withColumn("wait_value",F.expr("try_cast(queue_wait_minutes AS DECIMAL(10,4))")))
missing=F.lit(False)
for c in columns: missing=missing|F.col(c).isNull()|(F.trim(F.col(c))=="")
bad_time=(F.col("observed_at").isNull()|F.col("arrived_at").isNull()|
    ~F.col("event_time").rlike(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:00Z$")|
    ~F.col("arrival_time").rlike(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:00Z$")|
    (F.col("arrived_at")<F.col("observed_at")))
occ=F.col("occupancy_value"); wait=F.col("wait_value")
bad_occ=occ.isNull()|(occ<0)|(occ>F.col("capacity"))|(occ!=F.floor(occ))|~F.col("occupancy").rlike(r"^\d{1,4}$")
bad_wait=wait.isNull()|(wait<0)|(wait>120)|(wait!=F.round(wait,2))|~F.col("queue_wait_minutes").rlike(r"^\d{1,3}(\.\d{1,2})?$")
typed=typed.withColumn("quality_reason",F.when(missing,"missing_field").when(F.col("capacity").isNull(),"unknown_zone")
    .when(bad_time,"invalid_timestamp").when(bad_occ,"invalid_occupancy").when(bad_wait,"invalid_wait")
    .when(F.col("identity_conflict"),"identity_conflict"))
quarantine=typed.filter(F.col("quality_reason").isNotNull())
quarantine.write.format("delta").mode("overwrite").option("overwriteSchema",True).saveAsTable(f"{prefix}.quarantine_snapshots")
silver=(typed.filter(F.col("quality_reason").isNull()).select("event_id","zone","observed_at","arrived_at","capacity",
    F.col("occupancy_value").cast("int").alias("occupancy"),F.col("wait_value").cast("decimal(5,2)").alias("queue_wait_minutes")))
# Contract requires one unique snapshot per zone per minute; do not silently aggregate logical duplicates.
assert silver.count()==silver.select("zone","observed_at").distinct().count(), "Duplicate zone-minute snapshots"
assert silver.count()==silver.select("event_id").distinct().count(), "Duplicate accepted event IDs"
silver=(silver.withColumn("delay_seconds",F.unix_timestamp("arrived_at")-F.unix_timestamp("observed_at"))
    .withColumn("late_arrival",F.col("delay_seconds")>600))
silver.write.format("delta").mode("overwrite").option("overwriteSchema",True).saveAsTable(f"{prefix}.silver_snapshots")

# COMMAND ----------
windows=(silver.groupBy(F.window("observed_at","5 minutes"),"zone","capacity")
    .agg(F.count("*").alias("samples"),F.max("occupancy").alias("peak_occupancy"),
         F.round(F.avg("queue_wait_minutes"),2).alias("mean_wait_minutes"),
         F.sum(F.col("late_arrival").cast("int")).alias("late_samples"))
    .select("zone","capacity",F.col("window.start").alias("window_start"),F.col("window.end").alias("window_end"),
            "samples","peak_occupancy","mean_wait_minutes","late_samples")
    .withColumn("peak_ratio",F.round(F.col("peak_occupancy")/F.col("capacity"),4))
    .withColumn("complete",F.col("samples")==5)
    .withColumn("pressure",(F.col("peak_ratio")>=.85)|(F.col("mean_wait_minutes")>=12)))
windows.write.format("delta").mode("overwrite").option("overwriteSchema",True).saveAsTable(f"{prefix}.gold_zone_windows")
cutoff=F.to_timestamp(F.lit("2026-09-26T18:30:00Z"))
summary=silver.groupBy("zone","capacity").agg(F.count("*").alias("samples"),F.max("occupancy").alias("peak_occupancy"),
    F.round(F.avg(F.when(F.col("observed_at")<cutoff,F.col("queue_wait_minutes"))),2).alias("before_wait"),
    F.round(F.avg(F.when(F.col("observed_at")>=cutoff,F.col("queue_wait_minutes"))),2).alias("after_wait"),
    F.round(F.avg(F.when(F.col("observed_at")<cutoff,F.col("occupancy"))),2).alias("before_mean_occupancy"),
    F.round(F.avg(F.when(F.col("observed_at")>=cutoff,F.col("occupancy"))),2).alias("after_mean_occupancy"))
summary.write.format("delta").mode("overwrite").option("overwriteSchema",True).saveAsTable(f"{prefix}.gold_zone_summary")
assert windows.filter("samples > 5").count()==0
display(summary.orderBy("zone"));display(windows.filter("pressure OR NOT complete").orderBy("window_start","zone"))
display(quarantine.select("event_id","quality_reason"))

