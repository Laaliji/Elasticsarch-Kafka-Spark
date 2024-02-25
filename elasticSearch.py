import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, concat, col, avg, to_date, lit
from pyspark.sql.types import StructType, StringType, ArrayType, FloatType, IntegerType, DateType

# Initialize Spark session with necessary packages
spark = SparkSession.builder \
    .appName("KafkaConsumer") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.4,"
            "org.elasticsearch:elasticsearch-spark-30_2.12:7.17.14") \
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
    .getOrCreate()

# Kafka configuration
kafka_bootstrap_servers = 'localhost:9092'  # Adjust the Kafka broker address as needed
kafka_topic = 'movies'

# Elasticsearch configuration
es_nodes = 'localhost:9200'  # Replace with your Elasticsearch host
es_port = '9200'  # Replace with your Elasticsearch port
es_resource = 'movies'  # Define the Elasticsearch index and document type

# Define the schema for the incoming JSON messages
schema = StructType() \
    .add("genre_ids", ArrayType(IntegerType())) \
    .add("original_language", StringType()) \
    .add("overview", StringType()) \
    .add("popularity", FloatType()) \
    .add("release_date", DateType()) \
    .add("title", StringType()) \
    .add("vote_average", FloatType()) \
    .add("vote_count", IntegerType())

# Read messages from Kafka
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .load()

# Convert the value column to string and parse JSON data according to the defined schema
parsed_df = df \
    .selectExpr("CAST(value AS STRING)") \
    .select(from_json("value", schema).alias("data")) \
    .select("data.*")

# Data enrichment and transformation
transformed_df = parsed_df \
    .withColumn("description", concat(col("title"), lit(":"), col("overview"))) \
    .withColumn("release_date", to_date("release_date", "yyyy-MM-dd"))

# Write the DataFrame into Elasticsearch
query = transformed_df \
    .writeStream \
    .outputMode("append") \
    .format("org.elasticsearch.spark.sql") \
    .option("checkpointLocation", "/your_checkpoint_dir") \
    .option("es.nodes", es_nodes) \
    .option("es.port", es_port) \
    .option("es.resource", es_resource) \
    .start()

query.awaitTermination()