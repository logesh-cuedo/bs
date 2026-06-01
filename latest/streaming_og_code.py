# Databricks notebook source

import avro.schema
from pyspark.sql import SparkSession
from pyspark.sql.avro.functions import from_avro
from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, struct, row_number, from_utc_timestamp, to_date, struct, from_utc_timestamp, year, month, day, round
from pyspark.sql.types import StructType, StructField
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from databricks.sdk.runtime import dbutils

from datetime import datetime
import time
import pytz
import logging
import json

# COMMAND ----------

spark.conf.set("spark.databricks.delta.autoCompact", "auto")
spark.conf.set("spark.databricks.delta.optimizeWrite", "true")
spark.conf.set("spark.sql.streaming.kafka.useDeprecatedOffsetFetching" , "false")


# NEW OPTIMIZATIONS decreased IT BY 100
spark.conf.set("spark.sql.shuffle.partitions", "200")
# spark.sql.shuffle.partitions	- 32 -> 200
# spark.shuffle.service.enabled	- false - > true

spark.conf.set("spark.sql.streaming.minBatchesToRetain", "2") # spark.sql.streaming.minBatchesToRetain	10
# NEW OPTIMIZATIONS ADDED
spark.conf.set("spark.sql.streaming.stateStore.compression.codec", "zstd")
spark.conf.set("spark.sql.streaming.minBatchesToRetain", "2") # spark.sql.streaming.minBatchesToRetain	10
spark.conf.set("spark.sql.streaming.stateStore.maintenanceInterval", "2min")
spark.conf.set("spark.sql.streaming.stateStore.timeout", "10min")
spark.conf.set("spark.cleaner.ttl", "3600")  # Clean metadata older than 1 hour
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
spark.conf.set("delta.feature.liquidClustering.enabled", "true")
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")



# spark.conf.set("spark.executor.memory", "60g")
# spark.conf.set("spark.driver.memory", "14g")
# spark.conf.set("spark.driver.maxResultSize", "4g")


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()

schema_path = '../config/schemav1.avsc'
with open(schema_path, "r") as f:
    avro_schema = f.read()

json_file_path = '../config/rename_mapv1.json'
with open(json_file_path, 'r') as file:
    rename_map = json.load(file)

# COMMAND ----------


catalog = dbutils.widgets.get("catalog")
schema_silver = dbutils.widgets.get("schema")

iot_events_latest_table = f"{catalog}.{schema_silver}.iot_events_latest"
iot_events_latest_valid_table = f"{catalog}.{schema_silver}.iot_events_latest_valid_new"
iot_table = f"{catalog}.bronze.iot"

# COMMAND ----------

parsed_schema = avro.schema.parse(avro_schema)

expected_fields = set(rename_map.keys())

col_order = [field.name for field in parsed_schema.fields]
col_order += ["timeLatency", "consumedLatency", "day", "month", "year", "insert_date"]

kafkaParams = {
    "kafka.bootstrap.servers": "b-1.mskinternalprodcluste.3mco1i.c4.kafka.ap-south-1.amazonaws.com:9092,b-2.mskinternalprodcluste.3mco1i.c4.kafka.ap-south-1.amazonaws.com:9092,b-3.mskinternalprodcluste.3mco1i.c4.kafka.ap-south-1.amazonaws.com:9092",
    "subscribe": "normalized-iot-events",
    "startingOffsets": "latest",
    "failOnDataLoss": "false",
    "kafka.group.id": "databricks_dev_wrk"
}

# COMMAND ----------


# df = spark.readStream.format("kafka").options(**kafkaParams).option("maxOffsetsPerTrigger", 500000).load()
df = spark.readStream.format("kafka").options(**kafkaParams).load()
decoded_df = df.select(from_avro(col("value"), avro_schema, {"mode": "PERMISSIVE"}).alias("data")) # added mode permissive to handle malformed messages
df_latest = spark.readStream.format("kafka").options(**kafkaParams).load()
decoded_df_latest = df_latest.select(from_avro(col("value"), avro_schema, {"mode": "PERMISSIVE"}).alias("data")) # added mode permissive to handle malformed messages


iot_data = decoded_df.select("data.*") \
    .withColumn("timeLatency", round((col("pushedAt") - col("ts")) / (1000 * 60), 2) ) \
    .withColumn("consumedLatency", round((col("consumedAt") - col("pushedAt")) / (1000 * 60), 2) ) \
    .withColumn("ts", (col("ts") / 1000).cast("timestamp")) \
    .withColumn("ts", from_utc_timestamp("ts", "Asia/Kolkata")) \
    .withColumn("insert_date", to_date("ts")) \
    .withColumn("year", year("ts")) \
    .withColumn("month", month("ts")) \
    .withColumn("day", day("ts")) \
    .select(col_order)

latest_df = decoded_df_latest.select("data.*") \
    .withColumn("ts", (col("ts") / 1000).cast("timestamp")) \
    .withColumn("originalTs", (col("originalTs") / 1000).cast("timestamp")) \
    .withColumn("consumedAt", (col("consumedAt") / 1000).cast("timestamp")) \
    .withColumn("pushedAt", (col("pushedAt") / 1000).cast("timestamp")) \
    .withColumn("ts", from_utc_timestamp("ts", "Asia/Kolkata")) \
    .withColumn("originalTs", from_utc_timestamp("originalTs", "Asia/Kolkata")) \
    .withColumn("consumedAt", from_utc_timestamp("consumedAt", "Asia/Kolkata")) \
    .withColumn("pushedAt", from_utc_timestamp("pushedAt", "Asia/Kolkata"))



# COMMAND ----------

#check if delta table already exists
def delta_table_exists(spark, table_name):
    """
    Checks if a Delta table exists.

    Parameters:
    spark (SparkSession): The Spark session.
    table_name (str): The name of the Delta table.

    Returns:
    bool: True if the table exists, False otherwise.
    """
    exists = spark.catalog.tableExists(table_name)
    
    if exists:
        return True
    else:
        return False


def clear_catalog_cache(batch_id):
    print("Request received for(clear_catalog_cache) {batch_id}")
    if batch_id % 60 == 0:  # Clear cache every 60 batches
        print(f"Clearing catalog cache {batch_id}")
        spark.catalog.clearCache()

# COMMAND ----------

def process_iot(df, batch_id):
    # Handle unexpected fields
    alarm_fields = {field.name for field in df.schema["alarms"].dataType.fields}
    unexpected_fields = alarm_fields - expected_fields

    if unexpected_fields:
        logger.error(f"Unexpected fields found in alarms column: {unexpected_fields}")
        raise ValueError(f"Unexpected fields found in alarms column: {unexpected_fields}")

    # Convert alarms struct fields to string format
    struct_fields = [col(f"alarms.{field.name}").cast("string").alias(rename_map[field.name]) for field in df.schema["alarms"].dataType.fields]
    source_df = df.withColumn("alarms", struct(*struct_fields))

    # Write data with partitioning on year and month
    source_df.write.format('delta') \
        .mode("append") \
        .partitionBy("year", "month", "day") \
        .option("mergeSchema", "true") \
        .saveAsTable(iot_table)


# COMMAND ----------

def upsert_to_delta(batch_df, batch_id):
    window_spec = Window.partitionBy("deviceID").orderBy(col("ts").desc())
    
    latest_rows_df = batch_df.withColumn("row_num", row_number().over(window_spec)) \
                             .filter(col("row_num") == 1) \
                             .drop("row_num")
    # print("latest rows df", latest_rows_df)

    valid_df = batch_df.withColumn("isBMSValid", F.when((F.col("voltage")>0)&(F.col("voltage")<70),1).otherwise(0)) \
        .withColumn("isIoTValid", F.when((F.col("lon")>0),1).otherwise(0))

    bms_valid_df = valid_df.filter(col("isBMSValid") == 1).select("deviceID","ts","lat","lon","voltage","soc","current","temperature","cellVolt","cellTemp","alarms")
    iot_valid_df = valid_df.filter(col("isIoTValid") == 1).select("deviceID","ts","lat","lon")
    bms_valid_df = bms_valid_df.withColumn("row_num", row_number().over(window_spec)).filter(col("row_num")==1).drop("row_num").withColumnsRenamed({'ts':'bmsLastTs','lat':'bmsLastLat','lon':'bmsLastLon'})
    iot_valid_df = iot_valid_df.withColumn("row_num", row_number().over(window_spec)).filter(col("row_num")==1).drop("row_num").withColumnRenamed("ts","iotLastTs")
    latest = latest_rows_df.select("deviceID","ts")
    combined = latest.join(bms_valid_df, "deviceID","left").join(iot_valid_df, "deviceID","left")

    if delta_table_exists(spark, iot_events_latest_valid_table):
        delta_table_valid = DeltaTable.forName(spark, iot_events_latest_valid_table)
        insert_values_valid = {col_name: f"source.{col_name}" for col_name in combined.columns}

        delta_table_valid.alias("target").merge(
            combined.alias("source"),
            "target.deviceID = source.deviceID"
        ).whenMatchedUpdate(
            set={
                # ts update
                "ts": "CASE WHEN target.ts IS NULL OR source.ts > target.ts THEN source.ts ELSE target.ts END",

                # BMS updates
                "bmsLastTs": "CASE WHEN target.bmsLastTs IS NULL OR source.bmsLastTs > target.bmsLastTs THEN source.bmsLastTs ELSE target.bmsLastTs END",
                "voltage": "CASE WHEN target.bmsLastTs IS NULL OR source.bmsLastTs > target.bmsLastTs THEN source.voltage ELSE target.voltage END",
                "temperature": "CASE WHEN target.bmsLastTs IS NULL OR source.bmsLastTs > target.bmsLastTs THEN source.temperature ELSE target.temperature END",
                "current": "CASE WHEN target.bmsLastTs IS NULL OR source.bmsLastTs > target.bmsLastTs THEN source.current ELSE target.current END",
                "soc": "CASE WHEN target.bmsLastTs IS NULL OR source.bmsLastTs > target.bmsLastTs THEN source.soc ELSE target.soc END",
                "cellVolt": "CASE WHEN target.bmsLastTs IS NULL OR source.bmsLastTs > target.bmsLastTs THEN source.cellVolt ELSE target.cellVolt END",
                "cellTemp": "CASE WHEN target.bmsLastTs IS NULL OR source.bmsLastTs > target.bmsLastTs THEN source.cellTemp ELSE target.cellTemp END",
                "alarms": "CASE WHEN target.bmsLastTs IS NULL OR source.bmsLastTs > target.bmsLastTs THEN source.alarms ELSE target.alarms END",
                "bmsLastLat": "CASE WHEN target.bmsLastTs IS NULL OR source.bmsLastTs > target.bmsLastTs THEN source.bmsLastLat ELSE target.bmsLastLat END",
                "bmsLastLon": "CASE WHEN target.bmsLastTs IS NULL OR source.bmsLastTs > target.bmsLastTs THEN source.bmsLastLon ELSE target.bmsLastLon END",

                # IoT updates
                "iotLastTs": "CASE WHEN target.iotLastTs IS NULL OR source.iotLastTs > target.iotLastTs THEN source.iotLastTs ELSE target.iotLastTs END",
                "lat": "CASE WHEN target.iotLastTs IS NULL OR source.iotLastTs > target.iotLastTs THEN source.lat ELSE target.lat END",
                "lon": "CASE WHEN target.iotLastTs IS NULL OR source.iotLastTs > target.iotLastTs THEN source.lon ELSE target.lon END",
            }
        ).whenNotMatchedInsert(
            values=insert_values_valid
        ).execute()
    else:
        combined.write.format("delta").option("mergeSchema", "true").mode("overwrite").saveAsTable(iot_events_latest_valid_table)    

    # if delta_table_exists(spark, iot_events_latest_valid_table):
    #     delta_table_valid = DeltaTable.forName(spark, iot_events_latest_valid_table)
    #     insert_values_valid = {col_name: f"source.{col_name}" for col_name in combined.columns}

    #     delta_table_valid.alias("target").merge(
    #         combined.alias("source"),
    #         "target.deviceID = source.deviceID"
    #     ).whenMatchedUpdate(
    #         condition="target.ts IS NULL OR source.ts > target.ts",
    #         set={"target.ts":"source.ts"}
    #     ).whenMatchedUpdate(
    #         condition="target.bmsLastTs IS NULL OR source.bmsLastTs > target.bmsLastTs",
    #         set={"target.bmsLastTs":"source.ts","target.voltage":"source.voltage","target.temperature":"source.temperature","target.current":"source.current","target.soc":"source.soc","target.cellVolt":"source.cellVolt","target.cellTemp":"source.cellTemp","target.alarms":"source.alarms","target.bmsLastLat":"source.bmsLastLat","target.bmsLastLon":"source.bmsLastLon"}
    #     ).whenMatchedUpdate(
    #         condition="target.iotLastTs IS NULL OR source.iotLastTs > target.iotLastTs",
    #         set={"target.iotLastTs":"source.iotLastTs","target.lat":"source.lat","target.lon":"source.lon"}
    #     ).whenNotMatchedInsert(
    #         values=insert_values_valid
    #     ).execute()
    # else:
    #     combined.write.format("delta").option("mergeSchema", "true").mode("overwrite").saveAsTable(iot_events_latest_valid_table)    


    if delta_table_exists(spark, iot_events_latest_table):
        delta_table = DeltaTable.forName(spark, iot_events_latest_table)

        update_set = {col_name: f"source.{col_name}" for col_name in batch_df.columns}
        insert_values = {col_name: f"source.{col_name}" for col_name in batch_df.columns}
        delta_table.alias("target").merge(
            latest_rows_df.alias("source"),
            "target.deviceID = source.deviceID"
        ).whenMatchedUpdate(
            condition="source.ts > target.ts",
            set=update_set
        ).whenNotMatchedInsert(
            values=insert_values
        ).execute()
    else:
        latest_rows_df.write.format("delta").option("mergeSchema", "true").mode("overwrite").saveAsTable(iot_events_latest_table)



    # ######### CODE TO UPDATE IOT EVENTS LATEST IN ANALYTICS DB ###########

    # ist = pytz.timezone('Asia/Kolkata')

    # # Get current time in IST
    # current_time_ist = datetime.now(ist)

    # # Extract current minute
    # current_minute = current_time_ist.minute
    # print(current_minute)

    # if current_minute % 10 == 0:
    #     # Your code logic here
    #     print("Running code because current IST minute is multiple of 10")

    #     sqlConnection = pymysql.connect(host=hostname,
    #                                 user=username,
    #                                 password=password,
    #                                 database=database_name,
    #                                 cursorclass=pymysql.cursors.DictCursor)
    

    #     database_name = "analytics_prod"
    #     database_port = "3306"
    #     hostname = dbutils.secrets.get(scope = "jdbc", key = "analytics_db")
    #     username = dbutils.secrets.get(scope = "jdbc", key = "analytics_w_user")
    #     password = dbutils.secrets.get(scope = "jdbc", key = "analytics_w_pwd")
    #     url = f"jdbc:mysql://{hostname}:{database_port}/{database_name}"

    #     db_latest = spark.read.table("deltalake.silver.iot_events_latest")

    #     df_casted = db_latest.withColumn("ts", col("ts") - expr("INTERVAL 330 MINUTES")) \
    #         .withColumn("originalTs", col("originalTs") - expr("INTERVAL 330 MINUTES")) \
    #         .withColumn("pushedAt", col("pushedAt") - expr("INTERVAL 330 MINUTES")) \
    #         .withColumn("consumedAt", col("consumedAt") - expr("INTERVAL 330 MINUTES")) \
    #         .withColumn("caliberationVersion", lit(None)).select(
    #         col("eventId").cast("string"),
    #         col("canId").cast("string"),
    #         col("deviceID").cast("string"),
    #         col("ts").cast("timestamp"),
    #         col("originalTs").cast("timestamp"),
    #         col("pushedAt").cast("timestamp"),
    #         col("consumedAt").cast("timestamp"),
    #         col("altitude").cast("float"),
    #         col("soh").cast("float"),
    #         col("soc").cast("float"),
    #         col("sop").cast("float"),
    #         col("lat").cast("float"),
    #         col("lon").cast("float"),
    #         col("gsmLat").cast("float"),
    #         col("gsmLon").cast("float"),
    #         col("radius").cast("float"),
    #         col("dischargeCycles").cast("int"),
    #         col("current").cast("float"),
    #         col("temperature").cast("float"),
    #         col("heading").cast("float"),
    #         col("speed").cast("float"),
    #         col("voltage").cast("float"),
    #         col("cellVolt"),
    #         col("cellTemp"),
    #         col("alarms"),
    #         col("alarmEncoded").cast("string"),
    #         col("schemaVersion").cast("string"),
    #         col("chargeFetStatus").cast("int"),       
    #         col("dischargeFetStatus").cast("int"),  
    #         col("batteryCapacity").cast("float"),
    #         col("ambientSensorTemp").cast("float"),
    #         col("mosfetTemp").cast("float"),
    #         col("batteryManufacturerName").cast("string"),
    #         col("fgPartNum").cast("string"),
    #         col("softwareVersion").cast("string"),
    #         col("hardwareVersion").cast("string"),
    #         col("caliberationVersion").cast("string"),
    #         col("deltaTemperature").cast("float"),
    #         col("deltaCurrent").cast("float"),
    #         col("deltaVoltage").cast("float"),
    #         col("maxChargingVoltage").cast("float"),
    #         col("chargingStatus").cast("string"),
    #         col("chargeStatus").cast("string"),
    #         col("chargeCycle").cast("int"),
    #         col("maxChargingCurrent").cast("float")
    #     ).dropna(subset = ["deviceId"])

    #     struct_cols = ["cellVolt", "cellTemp", "alarms"]
    #     for c in struct_cols:
    #         df_casted = df_casted.withColumn(c, to_json(col(c)))

    #     jdbc_url = f"jdbc:mysql://{hostname}:{database_port}/{database_name}"


    #     table_name = "iotEventsLatestStaging"

    #     # connection_properties = {"user": username, "password": password}


    #     # (
    #     #     df_casted.write.mode("overwrite")  # overwrite existing data
    #     #     .option("truncate", "true")  # truncate instead of drop+create
    #     #     .jdbc(jdbc_url, table_name, properties=connection_properties)
    #     # )

    #     df_columns = df_casted.columns

    #     pk_column = 'deviceId'
    #     staging_table = 'iotEventsLatestStaging'
    #     target_table = 'iotEventsLatestTesting'
    #     conn = sqlConnection
    #     # Build SQL parts
    #     col_list = ", ".join(df_columns)


    #     value_list = ", ".join([f"s.{c}" for c in df_columns])
    #     print(value_list)
    #     update_list = ", ".join([f"{c}=VALUES({c})" for c in df_columns if c != pk_column])
    #     print(update_list)

    #     merge_sql = f"""
    #     INSERT INTO {target_table} ({col_list})
    #     SELECT {value_list}
    #     FROM {staging_table} s
    #     ON DUPLICATE KEY UPDATE {update_list};
    #     """


    #     batch_size = 20000
    #     offset = 0
    #     cursor = conn.cursor()

    #     while True:
    #         merge_sql = f"""
    #         INSERT INTO {target_table} ({col_list})
    #         SELECT {value_list}
    #         FROM {staging_table} s
    #         LIMIT {batch_size} OFFSET {offset}
    #         ON DUPLICATE KEY UPDATE {update_list};
    #         """
            
    #         cursor.execute(merge_sql)
    #         conn.commit()

    #         print(f"Executing SQL: {merge_sql}")
    #         print(f"Commited {cursor.rowcount} rows")
    #         time.sleep(1)

            
    #         if cursor.rowcount < batch_size:
    #             break
    #         offset += batch_size

    #     print(conn)
    #     conn.close()

    # else:
    #     print("Skipping execution since not on 10-minute mark")


    clear_catalog_cache(batch_id)

# COMMAND ----------

iot_query = iot_data.writeStream \
    .foreachBatch(process_iot) \
    .outputMode("append") \
    .option("checkpointLocation", "/mnt/delta/checkpoints/batterysmart_deltalake/bronze/iot") \
    .trigger(processingTime="30 seconds") \
    .start()

# COMMAND ----------

latest_query = latest_df.writeStream \
    .foreachBatch(upsert_to_delta) \
    .outputMode("update") \
    .option("checkpointLocation", "/mnt/delta/checkpoints/batterysmart_deltalake/silver/iot_events_latest_kafka1") \
    .option("mergeSchema", "true") \
    .trigger(processingTime="30 seconds") \
    .start()
