from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, rank
from pyspark.sql.window import Window

# Initialize Spark session
spark = SparkSession.builder.appName("AlternativeDates").getOrCreate()

# Load user logs
user_logs = spark.read.csv("s3://your-spark-bucket/mock-data/user_logs.csv", header=True, inferSchema=True)

# Load flight data
flights = spark.read.csv("s3://your-spark-bucket/mock-data/flights.csv", header=True, inferSchema=True)

# Join user searches with flights on origin and destination
recommendations = user_logs.join(
    flights,
    (user_logs.origin == flights.origin) & (user_logs.destination == flights.destination),
    "inner"
)

# Rank flights by price and availability for each user search
window_spec = Window.partitionBy("user_id").orderBy(col("price").asc(), col("availability").desc())
recommendations = recommendations.withColumn("rank", rank().over(window_spec))

# Filter for the best alternative flight (rank = 1)
recommendations = recommendations.filter(col("rank") == 1).select(
    "user_id",
    "departure_date",
    "price",
    "availability"
).withColumnRenamed("departure_date", "suggested_date")

# Save results
recommendations.write.csv("s3://your-spark-bucket/processed/alternative_dates.csv", header=True)

spark.stop()
