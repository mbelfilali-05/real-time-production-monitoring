
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    to_timestamp,
    when,
    lit,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    IntegerType,
)

BRONZE_PATH = "/opt/project/data/delta/bronze"
SILVER_PATH = "/opt/project/data/delta/silver"

# Structure attendue des événements industriels
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
    .appName("ProductionSilverProcessing")
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

# 1. Lire la table Bronze
bronze = spark.read.format("delta").load(BRONZE_PATH)

print("Evenements Bronze :", bronze.count())

# 2. Convertir le JSON en colonnes
parsed = (
    bronze
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

# 3. Valider les données
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

# 4. Supprimer les doublons
deduplicated = valid.dropDuplicates(["event_id"])

# 5. Calculer les anomalies
silver = (
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
        .when(col("temperature") > 90, "HIGH_TEMPERATURE")
        .when(col("vibration") > 6, "HIGH_VIBRATION")
        .otherwise("NORMAL")
    )
    .drop("timestamp")
)

# 6. Enregistrer dans Delta Silver
# Mode overwrite : reconstruit Silver depuis Bronze.
# Adapté à cette première version batch.
(
    silver.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(SILVER_PATH)
)

# 7. Vérifier le résultat
result = spark.read.format("delta").load(SILVER_PATH)

print("\n=== RESULTATS SILVER ===")
print("Evenements Silver :", result.count())

print("\n=== EXEMPLES ===")
result.select(
    "machine_id",
    "temperature",
    "vibration",
    "is_anomaly",
    "anomaly_type",
).show(10, truncate=False)

print("\n=== NOMBRE D'ANOMALIES ===")
result.groupBy("is_anomaly").count().show()

spark.stop()
