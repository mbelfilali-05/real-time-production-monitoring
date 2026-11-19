
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp

spark = (
    SparkSession.builder
    .appName("ProductionBronzeIngestion")
    .master("local[2]")
    .config(
        "spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension"
    )
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    )
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# Connexion à Kafka
kafka_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:29092")
    .option("subscribe", "production-events")
    .option("startingOffsets", "earliest")
    .load()
)

# Bronze : conserver les messages et les métadonnées Kafka
bronze = kafka_stream.select(
    col("value").cast("string").alias("raw_json"),
    col("topic"),
    col("partition"),
    col("offset"),
    col("timestamp").alias("kafka_timestamp"),
    current_timestamp().alias("ingestion_timestamp")
)

# Écriture continue dans Delta Lake
query = (
    bronze.writeStream
    .format("delta")
    .outputMode("append")
    .option(
        "checkpointLocation",
        "/opt/project/data/checkpoints/bronze"
    )
    .trigger(processingTime="5 seconds")
    .start("/opt/project/data/delta/bronze")
)

print("Bronze ingestion démarrée : Kafka -> Delta Lake")

query.awaitTermination()
