from pyspark.sql import SparkSession
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler

# Initialize Spark Session
spark = SparkSession.builder.appName("AudienceSegmentation").getOrCreate()

# Load the data from S3
s3_input_path = "s3://your-spark-bucket/mock-data/user_logs.csv"
data = spark.read.csv(s3_input_path, header=True, inferSchema=True)

# Preprocess the data (select relevant columns for clustering)
assembler = VectorAssembler(inputCols=["price_range"], outputCol="features")
data_with_features = assembler.transform(data)

# Apply KMeans Clustering
kmeans = KMeans(k=3, seed=1)
model = kmeans.fit(data_with_features)
predictions = model.transform(data_with_features)

# Save the output to S3
s3_output_path = "s3://your-spark-bucket/user_segments/"
predictions.select("user_id", "prediction").write.csv(s3_output_path, header=True)

# Stop the Spark Session
spark.stop()
