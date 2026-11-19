
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    to_timestamp,
    when,
    lit,
    count,
    sum as spark_sum,
    avg,
    max as spark_max,
    round as spark_round,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    IntegerType,
)
from delta.tables import DeltaTable

BRONZE_PATH = "/opt/project/data/delta/bronze"
SILVER_PATH = "/opt/project/data/delta/silver"
GOLD_PATH = "/opt/project/data/delta/gold"
CHECKPOINT_PATH = "/opt/project/data/checkpoints/medallion"

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

spark = (
    SparkSession.builder
    .appName("ProductionMedallionStreaming")
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


def process_batch(batch_df, batch_id):
    if batch_df.isEmpty():
        return

    print(f"\n=== TRAITEMENT MICRO-BATCH {batch_id} ===")

    # 1. Convertir les nouveaux JSON Bronze
    parsed = (
        batch_df
        .select(
            from_json(col("raw_json"), schema).alias("data"),
            col("kafka_timestamp"),
            col("ingestion_timestamp"),
        )
        .select(
            "data.*",
            "kafka_timestamp",
            "ingestion_timestamp",
        )
    )

    # 2. Valider les mesures
    valid = parsed.filter(
        col("event_id").isNotNull()
        & col("machine_id").isin("M01", "M02", "M03", "M04")
        & col("timestamp").isNotNull()
        & col("temperature").between(-20, 200)
        & col("vibration").between(0, 50)
        & col("pressure").between(0, 20)
        & (col("production_rate") >= 0)
        & (col("defect_count") >= 0)
    )

    # 3. Déduplication au sein du micro-batch
    deduplicated = valid.dropDuplicates(["event_id"])

    # 4. Détection des anomalies
    silver_batch = (
        deduplicated
        .withColumn(
            "event_timestamp",
            to_timestamp(col("timestamp"))
        )
        .withColumn(
            "is_anomaly",
            (col("temperature") > lit(90))
            | (col("vibration") > lit(6))
        )
        .withColumn(
            "anomaly_type",
            when(
                (col("temperature") > 90)
                & (col("vibration") > 6),
                "TEMPERATURE_AND_VIBRATION"
            )
            .when(
                col("temperature") > 90,
                "HIGH_TEMPERATURE"
            )
            .when(
                col("vibration") > 6,
                "HIGH_VIBRATION"
            )
            .otherwise("NORMAL")
        )
        .drop("timestamp")
    )

    # 5. Insérer uniquement les nouveaux event_id
    # MERGE rend la réexécution d'un micro-batch
    # idempotente pour la table Silver.
    target = DeltaTable.forPath(spark, SILVER_PATH)

    (
        target.alias("target")
        .merge(
            silver_batch.alias("source"),
            "target.event_id = source.event_id"
        )
        .whenNotMatchedInsertAll()
        .execute()
    )

    # 6. Recalculer les KPI Gold depuis Silver
    silver_all = spark.read.format("delta").load(SILVER_PATH)

    gold = (
        silver_all
        .groupBy("machine_id", "machine_type")
        .agg(
            count("*").alias("total_events"),
            spark_round(avg("temperature"), 2).alias(
                "avg_temperature"
            ),
            spark_round(avg("vibration"), 2).alias(
                "avg_vibration"
            ),
            spark_sum(
                when(col("is_anomaly"), 1).otherwise(0)
            ).alias("total_anomalies"),
            spark_sum("defect_count").alias(
                "total_defects"
            ),
            spark_round(
                avg("production_rate"), 2
            ).alias("avg_production_rate"),
            spark_max("event_timestamp").alias(
                "last_event_timestamp"
            ),
        )
        .withColumn(
            "anomaly_rate",
            spark_round(
                col("total_anomalies")
                / col("total_events") * 100,
                2
            )
        )
    )

    # 7. Publier une nouvelle version Gold
    (
        gold.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(GOLD_PATH)
    )

    print(f"Micro-batch {batch_id} termine")
    print("Silver et Gold actualises")


# 8. Lire les nouvelles versions de Bronze
bronze_stream = (
    spark.readStream
    .format("delta")
    .option("maxFilesPerTrigger", 10)
    .load(BRONZE_PATH)
)

# 9. Exécuter automatiquement le traitement
query = (
    bronze_stream.writeStream
    .foreachBatch(process_batch)
    .option("checkpointLocation", CHECKPOINT_PATH)
    .trigger(processingTime="10 seconds")
    .start()
)

print("Streaming Medallion demarre : Bronze -> Silver -> Gold")

query.awaitTermination()
