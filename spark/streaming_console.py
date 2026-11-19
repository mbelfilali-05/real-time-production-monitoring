
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    when,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    IntegerType,
)


# 1. Définir la structure des événements JSON
schema = StructType([
    StructField("event_id", StringType()),
    StructField("machine_id", StringType()),
    StructField("machine_type", StringType()),
    StructField("timestamp", StringType()),
    StructField("temperature", DoubleType()),
    StructField("vibration", DoubleType()),
    StructField("pressure", DoubleType()),
    StructField("production_rate", IntegerType()),
    StructField("defect_count", IntegerType()),
    StructField("status", StringType()),
])


# 2. Initialiser Spark
spark = (
    SparkSession.builder
    .appName("ProductionMonitoring")
    .master("local[2]")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# 3. Lire le flux Kafka
kafka_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:29092")
    .option("subscribe", "production-events")
    .option("startingOffsets", "earliest")
    .load()
)


# 4. Convertir les messages JSON en colonnes
events = (
    kafka_stream
    .selectExpr("CAST(value AS STRING) AS json_message")
    .select(from_json(col("json_message"), schema).alias("data"))
    .select("data.*")
)


# 5. Détecter les anomalies
processed = events.withColumn(
    "is_anomaly",
    when(
        (col("temperature") > 90) |
        (col("vibration") > 6),
        True,
    ).otherwise(False),
)


# 6. Afficher les résultats dans le terminal
query = (
    processed.writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", "false")
    .option("checkpointLocation", "/opt/project/data/checkpoints/console")
    .trigger(processingTime="5 seconds")
    .start()
)

print("Spark Streaming démarré. Lecture des événements Kafka...")

query.awaitTermination()
