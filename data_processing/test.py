from datetime import datetime, timedelta
import logging
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, regexp_extract, col, year, month, dayofmonth, hour, to_timestamp
from pyspark.sql.types import StructField, IntegerType, StringType, StructType, TimestampType, LongType
import boto3
import last_batch_time as lbt


if __name__ == '__main__':
    spark = SparkSession.builder \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4") \
        .appName("accesslog-processing-test01") \
        .getOrCreate()    
    
    dynamodb_table_name = "emr_last_batch_time"
    table_name = "access_log"
    dynamodb = boto3.client('dynamodb', region_name='ap-northeast-2')
    existing_tables = dynamodb.list_tables()['TableNames']
    if dynamodb_table_name in existing_tables:
        logging.info(f"DynamoDB Table '{dynamodb_table_name}' already exists.")
    else:
        lbt.create_dynamodb_table(dynamodb, dynamodb_table_name)

    dynamodb = boto3.resource('dynamodb', region_name="ap-northeast-2")
    dynamodb_table = dynamodb.Table(dynamodb_table_name)

    last_bookmark_time = lbt.get_last_bookmark(dynamodb_table, table_name)

    # Set the start time for reading data
    if last_bookmark_time:
        last_timestamp = datetime.strptime(
            last_bookmark_time, '%Y-%m-%d %H:%M:%S')
        # last_timestamp = last_bookmark_time
    else:
        # Default to epoch time if no last bookmark
        last_timestamp = datetime(2024, 6, 20, 14)

    current_time = datetime.now()
    # s3_path = f's3a://chiholee-datalake0002/msk/access_log_topic/year={last_timestamp.year}/month={last_timestamp.month:02}/day={last_timestamp.day:02}/hour={last_timestamp.hour:02}'


    # print("## s3_path : ", s3_path)

    # Read JSON files from S3 path
    # df = spark.read \
    #     .json(s3_path)

    df = None
    while last_timestamp < current_time:
        print("# last_timestamp : ", last_timestamp)
        print("# current_time : ", current_time)
        next_s3_path = f's3a://chiholee-datalake0002/msk/access_log_topic/year={last_timestamp.year}/month={last_timestamp.month:02}/day={last_timestamp.day:02}/hour={last_timestamp.hour:02}'
        print("## next_s3_path : ", next_s3_path)
        if df is None :
            df = spark.read.json(next_s3_path)
        else :
            df = df.union(spark.read.json(next_s3_path))
        next_timestamp = last_timestamp + timedelta(hours=1)
        last_timestamp = next_timestamp
        # break

    

    # Split the 'log' column into multiple columns using regex
    # |2024-06-15 14: 59: 59 | 171.40.213.109 - - "GET /products?product_id=18&customer_id=19 HTTP/1.1" 200 1576
    df = df.withColumn('timestamp', regexp_extract(col('log'), r'\|([^\|]+)\|', 1)) \
           .withColumn('ip_address', regexp_extract(col('log'), r'\|\s(\d+\.\d+\.\d+\.\d+)\s-\s-\s', 1)) \
           .withColumn('http_method', regexp_extract(col('log'), r'\"(GET|POST|PUT|DELETE)\s', 1)) \
           .withColumn('service', regexp_extract(col('log'), r'\"[A-Z]+\s/([^\s]+)\?', 1)) \
           .withColumn('endpoint', regexp_extract(col('log'), r'\"[A-Z]+\s(/[^\s]+)\s', 1)) \
           .withColumn('protocol', regexp_extract(col('log'), r'\"[A-Z]+\s/[^\s]+\s(HTTP/\d\.\d)\"', 1)) \
           .withColumn('status_code', regexp_extract(col('log'), r'\s(\d{3})\s', 1).cast(IntegerType())) \
           .withColumn('response_size', regexp_extract(col('log'), r'\s(\d+)$', 1).cast(IntegerType()))

    # Extract product_id, customer_id, order_id from the endpoint
    df = df.withColumn('product_id', regexp_extract(col('endpoint'), r'product_id=(\d+)', 1).cast(IntegerType())) \
           .withColumn('customer_id', regexp_extract(col('endpoint'), r'customer_id=(\d+)', 1).cast(IntegerType())) \
           .withColumn('order_id', regexp_extract(col('endpoint'), r'order_id=(\d+)', 1).cast(IntegerType()))

    df = df.withColumn('year', year(col('timestamp'))) \
           .withColumn('month', month(col('timestamp'))) \
           .withColumn('day', dayofmonth(col('timestamp'))) \
           .withColumn('hour', hour(col('timestamp')))
    
    # Select relevant columns
    df = df.select(
        col('timestamp'),
        col('ip_address'),
        col('http_method'),
        col('endpoint'),
        col('service'),
        col('protocol'),
        col('status_code'),
        col('response_size'),
        col('product_id'),
        col('customer_id'),
        col('order_id'),
        col('year'),
        col('month'),
        col('day'),
        col('hour')
    )

    # df.show(truncate=False)
    # df.printSchema()

    df.write \
      .partitionBy('year', 'month', 'day', 'hour') \
      .mode('overwrite') \
      .parquet('s3a://chiholee-datalake0002/msk/access_log_topic_1st_processing/')
    
    latest_timestamp = df.agg({"timestamp": "max"}).collect()[0][0]
    if latest_timestamp:
        lbt.set_last_bookmark(
            dynamodb_table, table_name, latest_timestamp)


