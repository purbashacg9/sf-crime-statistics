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
    ## TODO add validation for timestamp string 
    if ts:
        return datetime.strptime(ts, "%Y-%m-%dT%H:%M:%S.000")


format_timestamp_udf = psf.udf(format_timestamp, TimestampType())


# TODO Create a schema for incoming resources
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

    # TODO Create Spark Configuration
    # Create Spark configurations with max offset of 200 per trigger
    # set up correct bootstrap server and port
    # TODO Purbasha # could try to use startingOffsets, maxRatePerPartition as used in the lesson Integrating Spark and Kafka
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "org.udacity.assignment.crime-statistics") \
        .option("maxOffsetsPerTrigger", 200) \
        .option("startingOffsets", "earliest") \
        .option("stopGracefullyOnShutdown", "true") \
        .load() 

    # Show schema for the incoming resources for checks
    df.printSchema()

    # TODO extract the correct column from the kafka input resources
    # Take only value and convert it to String
    kafka_df = df.selectExpr("CAST(value AS STRING)")

    service_table = kafka_df\
        .select(psf.from_json(psf.col('value'), schema).alias("DF"))\
        .select("DF.*")
    
    # TODO select original_crime_type_name and disposition
    distinct_table = service_table\
                    .select("original_crime_type_name", "disposition", format_timestamp_udf("call_date_time").alias("call_date_time"))
    
#     query = distinct_table\
#             .writeStream\
#             .format("console")\
#             .outputMode("append") \
#             .trigger(once=True)\
#             .start() 
#     query.awaitTermination()

    # count the number of original crime type
    # TODO add .withWatermark("call_date_time", '60 minutes')\
    agg_df = (
        distinct_table\
        .withWatermark('call_date_time', '5 minutes')
        .groupBy
        (
            distinct_table.original_crime_type_name, 
            psf.window(distinct_table.call_date_time, "15 minutes", slideDuration="5 minutes")
        )
        .count()
        .select("original_crime_type_name", "window", psf.col("count").alias("count_original_crime_type"))
        .sort("count_original_crime_type", ascending=False)
    )            

    
    # TODO Q1. Submit a screen shot of a batch ingestion of the aggregation
    # TODO write output stream
    query = agg_df\
            .writeStream\
            .format("console")\
            .outputMode("complete") \
            .trigger(processingTime="15 minutes")\
            .option("truncate", "false")\
            .start() 
    
#processingTime="15 minutes"  once=True
#      #query = streamingCountsDF.writeStream.outputMode("complete").format("console").start()
#     # TODO attach a ProgressReporter
    query.awaitTermination()

#     # TODO get the right radio code json path
#     radio_code_json_filepath = "./radio_code.json"
#     radio_code_df = spark.read.json(radio_code_json_filepath, multiLine=True)

#     # clean up your data so that the column names match on radio_code_df and agg_df
#     # we will want to join on the disposition code

#     # TODO rename disposition_code column to disposition
#     radio_code_df = radio_code_df.withColumnRenamed("disposition_code", "disposition")

#     # TODO join on disposition column
#     join_query = agg_df.join(radio_code_df, agg_df.disposition=radio_code_df.disposition, 'inner')


#     join_query.awaitTermination()


if __name__ == "__main__":
    logger = logging.getLogger(__name__)

    # TODO Create Spark in Standalone mode
    spark = SparkSession \
        .builder \
        .master("local[*]") \
        .appName("KafkaSparkStructuredStreaming") \
        .getOrCreate()
    #spark.conf.set("spark.sql.shuffle.partitions", "1")
    #spark.sparkContext.setLogLevel("WARN") 

    logger.info("Spark started")

    run_spark_job(spark)

    spark.stop()
