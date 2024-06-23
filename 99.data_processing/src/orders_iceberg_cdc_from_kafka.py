from datetime import datetime, timedelta
import logging
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, regexp_extract, col, year, month, dayofmonth, hour, to_timestamp
from pyspark.sql.types import StructField, IntegerType, StringType, StructType, TimestampType, LongType
import boto3
import last_batch_time as lbt
import os


if __name__ == '__main__':

    catalog_name = "glue_catalog"
    bucket_name = "chiholee-datalake0002"
    database_name = "ecommerce"

    table_name = "orders"
    table_prefix = "from_kafka"
    last_update_time = 'order_dt'

    source_bucket_prefix = f"msk/rdb.{database_name}.{table_name}"
    source_path = f"s3a://{bucket_name}/{source_bucket_prefix}"

    iceberg_bucket_prefix = f"msk/iceberg/"
    warehouse_path = f"s3a://{bucket_name}/{iceberg_bucket_prefix}"

    ###########################################################################
    ## upsert 시 필요
    ## org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
    ###########################################################################

    

    # 실행 모드 확인
    is_cluster_mode = os.getenv("DEPLOY_MODE", "cluster")

    spark_builder = SparkSession.builder \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
        .config(f"spark.sql.catalog.{catalog_name}", "org.apache.iceberg.spark.SparkCatalog") \
        .config(f"spark.sql.catalog.{catalog_name}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
        .config(f"spark.sql.catalog.{catalog_name}.warehouse", f"{warehouse_path}") \
        .config("spark.sql.catalog.glue_catalog.lock-impl", "org.apache.iceberg.aws.dynamodb.DynamoDbLockManager") \
        .config("spark.sql.catalog.glue_catalog.lock.table", "IcebergLockTable") \
        .config("spark.jars.packages",
                ###########################################################
                ## iceberg 를 read 하기 위한 필수 jars
                ###########################################################
                "org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.5.2,"
                "software.amazon.awssdk:bundle:2.17.230,"
                ###########################################################
                "org.apache.hadoop:hadoop-aws:3.3.4,"
                # "org.apache.iceberg:iceberg-spark-extensions-3.4_2.12:1.5.2,"
                # "software.amazon.awssdk:url-connection-client:2.17.230,"
                # "org.apache.hadoop:hadoop-aws:3.3.4,"
                # "com.amazonaws:aws-java-sdk-bundle:1.11.901,"
                # "com.amazonaws:aws-java-sdk-core:1.12.725,"
                # "com.amazonaws:jmespath-java:1.12.725"
                ) \
        .appName("Iceberg CDC")

    # 클러스터 모드에서만 적용할 설정 추가
    if is_cluster_mode == "cluster":
        # local 에서 아래 옵션은 bug가 있다.
        # .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        print("## Cluster Mode : True")        
        spark_builder = spark_builder.config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    else :
        print("# Cluster Mode : False")

    # SparkSession 생성
    spark = spark_builder.getOrCreate()
    
    dynamodb_table_name = "emr_last_batch_time"
    dynamodb_table_name = "emr_last_batch_time"
    dynamodb = boto3.client('dynamodb', region_name='ap-northeast-2')
    existing_tables = dynamodb.list_tables()['TableNames']
    if dynamodb_table_name in existing_tables:
        logging.info(f"DynamoDB Table '{dynamodb_table_name}' already exists.")
    else:
        lbt.create_dynamodb_table(dynamodb, dynamodb_table_name)

    dynamodb = boto3.resource('dynamodb', region_name="ap-northeast-2")
    dynamodb_table = dynamodb.Table(dynamodb_table_name)

    last_bookmark_time = lbt.get_last_bookmark(dynamodb_table, f"{table_name}_{table_prefix}")

    if last_bookmark_time:
        last_timestamp = datetime.strptime(
            last_bookmark_time, '%Y-%m-%d %H:%M:%S')
    else:
        # s3의 파티션된 최소 데이터
        last_timestamp = datetime(2024, 6, 22, 23)

    current_time = datetime.now()
    

    df = None
    last_timestamp_ymdh = datetime.strftime(last_timestamp, '%Y%m%d%H')
    current_time_ymdh = datetime.strftime(current_time, '%Y%m%d%H')
    while last_timestamp_ymdh <= current_time_ymdh:
        print("# last_timestamp : ", last_timestamp)
        print("# current_time : ", current_time)
        next_s3_path = f's3a://chiholee-datalake0002/msk/rdb.ecommerce.orders/year={last_timestamp.year}/month={last_timestamp.month:02}/day={last_timestamp.day:02}/hour={last_timestamp.hour:02}'        
        print("## next_s3_path : ", next_s3_path)
        
        try:
            if df is None:
                df = spark.read.parquet(next_s3_path)
                # break
            else:
                df = df.union(spark.read.parquet(next_s3_path))
        except Exception as e:
            print(f"Error reading {next_s3_path}: {e}")

        next_timestamp = last_timestamp + timedelta(hours=1)
        last_timestamp = next_timestamp

        last_timestamp_ymdh = datetime.strftime(last_timestamp, '%Y%m%d%H')


    # df.show()

    df.createOrReplaceTempView("v_cdc_source")

    # last_processed_date를 datetime 객체로 변환 (이미 datetime 형태일 수 있음)
    # last_processed_date = datetime.strptime(last_bookmark_time, '%Y-%m-%dT%H:%M:%S.%f')
    # last_processed_date에서 1분을 빼기
    
    if last_bookmark_time :
        time_threshold = datetime.strptime(last_bookmark_time,'%Y-%m-%d %H:%M:%S')
    else :
        time_threshold = datetime(1990, 1, 1, 1)
    # 날짜 조건이 있는 쿼리
    query = f"""
    select  order_id,
            promo_id,
            order_cnt,
            order_price,
            order_dt,
            last_update_time,
            customer_id,
            product_id,
            year(order_dt) as year,
            month(order_dt) as month,
            dayofmonth(order_dt) as day,
            hour(order_dt) as hour
    from v_cdc_source
    where (order_id, order_dt) in
    (
        select order_id, max(order_dt) max_op_time
        from v_cdc_source
        WHERE order_dt >= '{time_threshold.strftime('%Y-%m-%d %H:%M:%S')}'
        group by order_id
    )
    """

    cdc_max_df = spark.sql(query)

    # cdc_max_df.show()
    cdc_max_df.createOrReplaceTempView("v_cdc_max")
    # cdc_max_df.show()

    # spark.sql(f"""
    #             select * from {catalog_name}.{database_name}.{table_name}_{table_prefix}
    #           """).show()

    spark.sql(f"""MERGE INTO {catalog_name}.{database_name}.{table_name}_{table_prefix} t
        USING v_cdc_max s ON s.order_id = t.order_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """)

    # latest_timestamp = cdc_max_df.agg({"order_dt": "max"}).collect()[0]["max(order_dt)"]
    latest_timestamp = cdc_max_df.agg({"order_dt": "max"}).collect()[0][0]
    if latest_timestamp:
        lbt.set_last_bookmark(
            dynamodb_table, f"{table_name}_{table_prefix}", latest_timestamp)

    spark.stop()
