## Project Overview

This project was a part of Udacity's Data Streaming Nanodegree. In this project, we were  provided with a real-world dataset, extracted from Kaggle, on San Francisco crime incidents.   

The goal of the project was to provide statistical analyses of the data using Apache Spark Structured Streaming. We were required to create a Kafka server to produce data, and ingest data through Spark Structured Streaming.

## Development Environment
    Spark 2.4.3 or above
    Scala 2.11.x
    Java 1.8.x
    Kafka build with Scala 2.11.x
    Python 3.6.x or 3.7.x

## Screenshots 
The screenshots.zip has all the screenshots requested in the project. 

- Screenshot of kafka-consumer-console output - kafka-console-consumer-output.png

- Screenshot of progress reporter after executing a Spark job - 
    - Progress reporter and aggregated results - aggregation_by_crime_type_data_progress_report.png
    - Progress reported and aggregated results with join - aggregation_by_crime_type_join_by_disposition_data_progress_report.png 

- Screenshot of the Spark Streaming UI as the streaming continues - Multiple files 
    - Screenshot of Executors tab - SparkUI-Executor.png
    - Screenshot of Stages tab - SparkUI-Stages.png 
    - Screenshot of Jobs tab - SparkUI-Jobs.png

## Performance Tuning of Spark Environment
 
As part of the project, we were asked to study the effect of modifying configuration parameters for Spark sessions to understand their impact on the throughput and latency of the data. Here are the answers to the questions posed-  

1. How did changing values on the SparkSession property parameters affect the throughput and latency of the data?

Increasing the executor and driver memory (spark.executor.memory, spark.driver.memory) from the default values of 1 GB to 2GB for the executor and driver, improved the number of rows processed per second. With the default values, the number of rows processed per second was  in the range of 10-15. After increasing the memory, the number of rows processed per second went up to 70 to 80 per second. 
I put the spark.default.parallelism value to 4 and that could have also had an impact on the throughput. Increasing the memory values also brought down the time to completion in the tasks from a few seconds to 10s of miliseconds. 

2. What were the 2-3 most efficient SparkSession property key/value pairs? Through testing multiple variations on values, how can you tell these were the most optimal?

The most efficient SparkSession properties were spark.executor.memory and spark.driver.memory. . The default value for spark.sql.shuffle.partitions according to documentation is 200. I reduced it to 10 but did not see much of an improvement. When I reduced it to 1, there was a definite increase in the performance of the application. In addition to this when I changed the parallelism to 4 I saw further improvement. 

Setting spark.eventLog.enabled to False appeared to have a slight improvement in the performance of the application but not as much as changing the other parameters. 
