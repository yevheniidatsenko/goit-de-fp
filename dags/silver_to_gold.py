from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, current_timestamp
import os


spark = SparkSession.builder \
    .appName("SilverToGold") \
    .getOrCreate()

athlete_bio_df = spark.read.parquet("/tmp/silver/athlete_bio")
athlete_event_results_df = spark.read.parquet("/tmp/silver/athlete_event_results")

# Renaming columns to avoid ambiguity
athlete_bio_df = athlete_bio_df.withColumnRenamed("country_noc", "bio_country_noc")
# Joining tables on the athlete_id column
joined_df = athlete_event_results_df.join(athlete_bio_df, "athlete_id")

# Calculating average values
aggregated_df = joined_df.groupBy("sport", "medal", "sex", "country_noc") \
    .agg(
        avg("height").alias("avg_height"),
        avg("weight").alias("avg_weight"),
        current_timestamp().alias("timestamp")
    )

output_path = "/tmp/gold/avg_stats"
os.makedirs(output_path, exist_ok=True)
aggregated_df.write.mode("overwrite").parquet(output_path)

print(f"Data saved to {output_path}")
df = spark.read.parquet(output_path)
df.show(truncate=False)

spark.stop()