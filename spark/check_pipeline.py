
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("CheckPipeline")
    .master("local[2]")
    .config(
        "spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension"
    )
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

for layer in ["bronze", "silver"]:
    df = spark.read.format("delta").load(
        f"/opt/project/data/delta/{layer}"
    )
    print(f"{layer.upper()} : {df.count()} evenements")

gold = spark.read.format("delta").load(
    "/opt/project/data/delta/gold"
)

print("\n=== KPI GOLD ===")
gold.select(
    "machine_id",
    "total_events",
    "total_anomalies",
    "anomaly_rate"
).orderBy("machine_id").show()

spark.stop()
