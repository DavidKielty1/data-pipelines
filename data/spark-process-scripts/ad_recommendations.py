from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import collect_set, when, concat_ws, rank, col

# Initialize Spark session
spark = SparkSession.builder.appName("AdRecommendations").getOrCreate()

# Load user segments
user_segments = spark.read.csv(
    "s3://your-spark-bucket/processed/user_segments.csv",
    header=True,
    inferSchema=True
)

# Map numeric predictions to textual segments
user_segments = user_segments.withColumn(
    "segment",
    when(user_segments.prediction == 0, "Business Class")
    .when(user_segments.prediction == 1, "Budget Travelers")
    .when(user_segments.prediction == 2, "Frequent Travelers")
)

# Load ad inventory
ads = spark.read.csv("s3://your-spark-bucket/mock-data/ads.csv", header=True, inferSchema=True)

# Join user segments with ads on target segment
ad_recommendations = user_segments.join(
    ads,
    user_segments.segment == ads.target_segment,
    "inner"
)

# Rank ads for each user by CTR
window_spec = Window.partitionBy("user_id").orderBy(col("click_through_rate").desc())

ad_recommendations = ad_recommendations.withColumn("rank", rank().over(window_spec))

# Filter top 5 ads per user
ad_recommendations = ad_recommendations.filter(col("rank") <= 5).drop("rank")

# Aggregate recommended ads for each user
ad_recommendations = ad_recommendations.groupBy("user_id").agg(
    concat_ws(", ", collect_set("ad_content")).alias("recommended_ads")
)

# Coalesce the DataFrame to a single partition
ad_recommendations = ad_recommendations.coalesce(1)

# Save results to a single CSV file
ad_recommendations.write.mode("overwrite").csv("s3://your-spark-bucket/processed/ad_recommendations.csv", header=True)

spark.stop()
