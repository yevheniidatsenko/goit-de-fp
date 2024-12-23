from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, lower
from pyspark.sql.types import StringType
import os

# Create a Spark session with the name "BronzeToSilver"
spark = SparkSession.builder \
    .appName("BronzeToSilver") \
    .getOrCreate()

def clean_text(df):
    for column in df.columns:
        if df.schema[column].dataType == StringType():
            df = df.withColumn(column, trim(lower(col(column))))
    return df

# List of tables to process
tables = ["athlete_bio", "athlete_event_results"]

# Loop through and process each table
for table in tables:
    df = spark.read.parquet(f"/tmp/bronze/{table}")
    
    df = clean_text(df)
    df = df.dropDuplicates()
    
    # Save the processed data to the silver directory
    output_path = f"/tmp/silver/{table}"
    os.makedirs(output_path, exist_ok=True)
    df.write.mode("overwrite").parquet(output_path)
    
    # Confirm and display saved data
    print(f"Data saved to {output_path}")
    df = spark.read.parquet(output_path)
    df.show(truncate=False)

# Stop the Spark session
spark.stop()