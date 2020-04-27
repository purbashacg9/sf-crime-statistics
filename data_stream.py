import logging
import json
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as psf

def format_timestamp(ts):
    """
    Given a timestamp of the form - "2018-12-31T23:57:00.000", return a 
    datetime.datetime object
    """ 
    if ts:
        return datetime.strptime(ts, "%Y-%m-%dT%H:%M:%S.000")

format_timestamp_udf = psf.udf(format_timestamp, TimestampType())

# Create a schema for incoming resources
schema = StructType([
    StructField("address", StringType(), True),
    StructField("address_type", StringType(), True),
    StructField("agency_id", StringType(), True),
    StructField("call_date", StringType(), True),
    StructField("call_date_time", StringType(), True),
    StructField("call_time", StringType(), True),
    StructField("city", StringType(), True),
    StructField("common_location", StringType(), True),
    StructField("crime_id", StringType(), True),
    StructField("disposition", StringType(), True),
    StructField("offense_date", StringType(), True),
    StructField("original_crime_type_name", StringType(), True),
    StructField("report_date", StringType(), True),
    StructField("state", StringType(), True),
])

def run_spark_job(spark):
    # Create Spark Configuration
    # Create Spark configurations with max offset of 200 per trigger
    # Set up correct bootstrap server and port
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "org.udacity.assignment.crime-statistics") \
        .option("maxOffsetsPerTrigger", 100) \
        .option("startingOffsets", "earliest") \
        .option("stopGracefullyOnShutdown", "true") \
        .load() 

    # Show schema for the incoming resources for checks
    df.printSchema()

    # Extract the correct column from the kafka input resources
    # Take only value and convert it to String
    kafka_df = df.selectExpr("CAST(value AS STRING)")

    service_table = kafka_df\
        .select(psf.from_json(psf.col('value'), schema).alias("DF"))\
        .select("DF.*")
    
    # Select original_crime_type_name and disposition
    distinct_table = service_table\
                    .select("original_crime_type_name", 
                            "disposition", 
                            format_timestamp_udf("call_date_time").alias("call_date_time")
                           )

    # count the number of original crime type
    agg_df = (
        distinct_table\
        .withWatermark('call_date_time', '60 minutes')
        .groupBy
        (
            distinct_table.original_crime_type_name, distinct_table.disposition,
            psf.window(distinct_table.call_date_time, "15 minutes", "5 minutes")
        )
        .count()
        .select("original_crime_type_name", "disposition", "window", psf.col("count").alias("count_original_crime_type"))
    )            
    
    # Q1. Submit a screen shot of a batch ingestion of the aggregation
    # Write output stream
    query = agg_df\
            .writeStream\
            .queryName("count_by_crime_type")\
            .format("console")\
            .outputMode("complete") \
            .trigger(processingTime="30 seconds")\
            .option("truncate", "false")\
            .start() 
    
    # Attach a ProgressReporter - Having 2 progress reporters in the code prevents the join query from running. 
    # So I commented this and retained the one for join_query.
    #query.awaitTermination()

    # Get the right radio code json path
    radio_code_json_filepath = "./radio_code.json"
    radio_code_df = spark.read.json(radio_code_json_filepath, multiLine=True)

    # clean up your data so that the column names match on radio_code_df and agg_df
    # we will want to join on the disposition code

    # Rename disposition_code column to disposition
    radio_code_df = radio_code_df.withColumnRenamed("disposition_code", "disposition")

    # join on disposition column
    join_query = agg_df.join(radio_code_df, "disposition", 'left_outer')\
        .select("original_crime_type_name", "description", "window","count_original_crime_type")\
        .writeStream\
        .queryName("count_by_crime_type_with_disposition")\
        .format("console")\
        .outputMode("complete") \
        .trigger(processingTime="30 seconds")\
        .option("truncate", "false")\
        .start() 

    join_query.awaitTermination()


if __name__ == "__main__":
    logger = logging.getLogger(__name__)

    # Create Spark in Standalone mode
    spark = SparkSession \
        .builder \
        .master("local[*]") \
        .appName("KafkaSparkStructuredStreaming") \
        .getOrCreate()
    spark.conf.set("spark.sql.shuffle.partitions", "1")

    logger.info("Spark started")

    run_spark_job(spark)

    spark.stop()
