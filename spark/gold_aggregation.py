
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    sum as spark_sum,
    avg,
    max as spark_max,
    round as spark_round,
    when,
)

SILVER_PATH = "/opt/project/data/delta/silver"
GOLD_PATH = "/opt/project/data/delta/gold"

spark = (
    SparkSession.builder
    .appName("ProductionGoldAggregation")
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

spark.sparkContext.setLogLevel("ERROR")

# 1. Lire les données Silver
silver = spark.read.format("delta").load(SILVER_PATH)

# 2. Calculer les KPI par machine
gold = (
    silver
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
        spark_sum("defect_count").alias("total_defects"),
        spark_round(avg("production_rate"), 2).alias(
            "avg_production_rate"
        ),
        spark_max("event_timestamp").alias(
            "last_event_timestamp"
        ),
    )
    .withColumn(
        "anomaly_rate",
        spark_round(
            col("total_anomalies") / col("total_events") * 100,
            2
        )
    )
)

# 3. Enregistrer les KPI dans Delta Gold
(
    gold.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(GOLD_PATH)
)

# 4. Afficher les résultats
result = spark.read.format("delta").load(GOLD_PATH)

print("\n=== KPI GOLD PAR MACHINE ===")
result.orderBy("machine_id").show(truncate=False)

print("\n=== MACHINE AVEC LE PLUS D'ANOMALIES ===")
result.orderBy(
    col("total_anomalies").desc()
).select(
    "machine_id",
    "machine_type",
    "total_anomalies",
    "anomaly_rate"
).show(1, truncate=False)

spark.stop()
