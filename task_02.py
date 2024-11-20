from pyspark.sql import SparkSession

# create Spark session
spark = (
    SparkSession.builder.master("local[*]")
    .config("spark.sql.shuffle.partitions", "2")
    .appName("SparkUITestSandbox")
    .getOrCreate()
)

# load dataset
nuek_df = (
    spark.read.option("header", "true")
    .option("inferSchema", "true")
    .csv("./data/nuek-vuh3.csv")
)

nuek_repart = nuek_df.repartition(2)

nuek_processed = (
    nuek_repart.where("final_priority < 3")
    .select("unit_id", "final_priority")
    .groupBy("unit_id")
    .count()
)

# add intermediate action: collect
nuek_processed.collect()

nuek_processed = nuek_processed.where("count > 2")

nuek_processed.collect()

input("Press Enter to continue ...")

# close Spark session
spark.stop()
