
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("DeltaLakeTest")
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

data = [
    ("M01", 72.5),
    ("M02", 105.3),
    ("M03", 68.2),
]

df = spark.createDataFrame(
    data,
    ["machine_id", "temperature"]
)

path = "/opt/project/data/delta/test_table"

df.write.format("delta").mode("overwrite").save(path)

print("\n=== DONNEES DELTA LAKE ===")

spark.read.format("delta").load(path).show()

spark.stop()
