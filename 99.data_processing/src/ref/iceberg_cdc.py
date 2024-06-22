from datetime import datetime, timedelta

import boto3
from botocore.exceptions import ClientError
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, IntegerType, StringType, StructType, TimestampType, LongType
from pyspark.sql.functions import dayofmonth, month, year
from pyspark.errors.exceptions.captured import AnalysisException
import logging

TEST_YN = False

# spark-submit 시 logging 레벨 때문에 에러 발생
LOGGING_LEVEL = logging.INFO

color_log_format = "\033[1;32m%(levelname)s\033[1;0m: %(message)s"
logging.basicConfig(format=color_log_format,
                    datefmt='%m/%d %I:%M:%S %p',
                    level=LOGGING_LEVEL)

def create_dynamodb_table(dynamodb, table_name):
    # 테이블이 없으면 생성
    try:
        table = dynamodb.create_table(
            TableName=table_name,
            KeySchema=[
                {
                    'AttributeName': 'table_name',
                    'KeyType': 'HASH'  # 파티션 키
                }
            ],
            AttributeDefinitions=[
                {
                    'AttributeName': 'table_name',
                    'AttributeType': 'S'  # 'S'는 문자열
                }
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 5,
                'WriteCapacityUnits': 5
            }
        )

        # 테이블이 생성될 때까지 기다림
        dynamodb.get_waiter('table_exists').wait(TableName=table_name)
        logging.info(f"# DynamoDB Table '{table_name}' has been created.")

    except Exception as e:
        logging.info(f"Error creating DynamoDB Table: {e}")


def get_last_bookmark(dynamodb_table, table_name):
    response = dynamodb_table.get_item(
        Key={'table_name': table_name}
    )
    if 'Item' in response:
        last_bookmark_time = datetime.strptime(response['Item']['last_bookmark_time'], '%Y-%m-%d %H:%M:%S')
        return last_bookmark_time
    else:
        logging.info(f"# No Data in {table_name} {dynamodb_table}")
        return None


def set_last_bookmark(dynamodb_table, table_name, last_bookmark_time):
    response = dynamodb_table.update_item(
        Key={'table_name': table_name},
        UpdateExpression="SET last_bookmark_time = :last_bookmark_time, last_update_time = :last_update_time",
        # UpdateExpression="SET StartDate = :startdate, LastProcessedDate = :lastprocesseddate",
        ExpressionAttributeValues={
            ':last_bookmark_time': last_bookmark_time.strftime('%Y-%m-%d %H:%M:%S'),
            ':last_update_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        },
        ReturnValues="UPDATED_NEW"
    )
    logging.info(f"Update succeeded: {response}")


def get_processing_date(last_bookmark_time):
    if last_bookmark_time is None:
        start_date = datetime.now() - timedelta(days=365)
    else:
        # UTC 시간으로 변경
        # 왜? DMS 의 partition 기능에 따라 s3에 트랜잭션 데이터 저장 시
        # UTC 기준으로 저장이 되므로
        # 만약 오전 4/24 07:00 로 bookmark 가 되어 있다면
        # 24일을 찾는게 맞겠지만 실제 s3 에는 9시간 전인 23일에 해당 데이터는 저장되어 있다.
        last_bookmark_time = last_bookmark_time - timedelta(hours=9)
        start_date = last_bookmark_time
    # replace 가 사용된 이유
    # 먄약 last_bookmark_time 이 4/24 13:00 이고 end_date 가 4/25 11:00 이면
    # 13:00 > 11:00 이므로 25일이 제외된다.
    end_date = datetime.now().replace(hour=23, minute=59, second=59, microsecond=0)
    while start_date <= end_date:
        yield f"{start_date.strftime('%Y/%m/%d/')}"
        start_date += timedelta(days=1)


if __name__ == '__main__':



    env = "staging"
    catalog_name = "glue_catalog"
    bucket_name = "chiholee-datalake001"
    soruce_database_name = f"ecommerce"
    target_database_name = f"{env}_ecommerce"

    table_name = "orders"
    last_update_time = 'order_dt'

    source_bucket_prefix = "transaction/cdc/raw"
    source_path = f"s3a://{bucket_name}/{source_bucket_prefix}"

    iceberg_bucket_prefix = f"{env}/transaction/"
    warehouse_path = f"s3a://{bucket_name}/{iceberg_bucket_prefix}"

    ###########################################################################
    ## upsert 시 필요
    ## org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
    ###########################################################################

    # local 에서 아래 옵션은 bug가 있다.
    # .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    spark = SparkSession.builder \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
        .config(f"spark.sql.catalog.{catalog_name}", "org.apache.iceberg.spark.SparkCatalog") \
        .config(f"spark.sql.catalog.{catalog_name}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
        .config(f"spark.sql.catalog.{catalog_name}.warehouse", f"{warehouse_path}") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
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
        .appName("Iceberg CDC") \
        .getOrCreate()

    dynamodb_table_name = "IcebergBookmark"
    dynamodb = boto3.client('dynamodb', region_name='ap-northeast-2')
    existing_tables = dynamodb.list_tables()['TableNames']
    if dynamodb_table_name in existing_tables:
        logging.info(f"DynamoDB Table '{dynamodb_table_name}' already exists.")
    else:
        create_dynamodb_table(dynamodb, dynamodb_table_name)

    dynamodb = boto3.resource('dynamodb', region_name="ap-northeast-2")
    dynamodb_table = dynamodb.Table(dynamodb_table_name)

    last_bookmark_time = get_last_bookmark(dynamodb_table, table_name)

    logging.info(f"# get_last_bookmark last_bookmark_time : {last_bookmark_time}")

    list_path = []

    if last_bookmark_time:
        list_path = list(get_processing_date(last_bookmark_time))
    else:
        list_path = list(get_processing_date(None))

    logging.info(list_path)

    cdc_source_df = None
    for path in list_path:
        try:
            # logging.info(f"{source_path}/{soruce_database_name}/{table_name}/{path}")
            df = spark.read.parquet(f"{source_path}/{soruce_database_name}/{table_name}/{path}")

            if cdc_source_df is None:
                cdc_source_df = df
            else:
                cdc_source_df = cdc_source_df.union(df)
                if TEST_YN:
                    break
        except AnalysisException as e:
            # logging.info(f"Error reading data from {path}: {e}")
            pass

    # cdc_source_df.show()

    cdc_source_df.createOrReplaceTempView("v_cdc_source")



    # dms 에서 파티션되어 들어갈때 utc로 들어간것 같음. 해결해야 함
    # spark.sql("select * from v_cdc_source where order_dt in (select max(order_dt) from v_cdc_source)").show()

    if last_bookmark_time is not None:
        # last_processed_date를 datetime 객체로 변환 (이미 datetime 형태일 수 있음)
        # last_processed_date = datetime.strptime(last_bookmark_time, '%Y-%m-%dT%H:%M:%S.%f')
        # last_processed_date에서 1분을 빼기
        time_threshold = last_bookmark_time - timedelta(minutes=1)
        # 날짜 조건이 있는 쿼리
        query = f"""
        select  order_id,
                promo_id,
                order_cnt,
                order_price,
                order_dt,
                customer_id,
                product_id,
                year(order_dt) as year,
                month(order_dt) as month,
                dayofmonth(order_dt) as day
        from v_cdc_source
        where (order_id, order_dt) in
        (
            select order_id, max(order_dt) max_op_time
            from v_cdc_source
            WHERE order_dt >= '{time_threshold.strftime('%Y-%m-%d %H:%M:%S')}'
            group by order_id
        )
        """
    else:
        # 날짜 조건이 없는 쿼리
        query = """
        select  order_id,
                promo_id,
                order_cnt,
                order_price,
                order_dt,
                customer_id,
                product_id,
                year(order_dt) as year,
                month(order_dt) as month,
                dayofmonth(order_dt) as day
        from v_cdc_source
        where (order_id, order_dt) in
        (
            select order_id, max(order_dt) max_op_time
            from v_cdc_source
            group by order_id
        )
        """

    cdc_max_df = spark.sql(query)

    # cdc_max_df.show()
    cdc_max_df.createOrReplaceTempView("v_cdc_max")

    spark.sql(f"""MERGE INTO {catalog_name}.{target_database_name}.{table_name} t
        USING v_cdc_max s ON s.order_id = t.order_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """)

    last_bookmark_time = cdc_max_df.agg({"order_dt": "max"}).collect()[0]["max(order_dt)"]

    logging.info(f"# last_bookmark_time : {last_bookmark_time}")

    set_last_bookmark(dynamodb_table, table_name, last_bookmark_time)

    spark.stop()
