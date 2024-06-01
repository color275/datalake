# spark-submit --deploy-mode client --master yarn orders_cdc_upsert_iceberg.py
# spark-submit --deploy-mode cluster --master yarn orders_cdc_upsert_iceberg.py
# spark-submit --deploy-mode cluster --master yarn \
# --num-executors 1 \
# --executor-cores 2 \
# --executor-memory 2g \
# orders_cdc_upsert_iceberg.py

from pyspark.sql import SparkSession
from pyspark.sql import SparkSession
import boto3
from datetime import datetime
from datetime import datetime, timedelta
from botocore.exceptions import ClientError

def create_table(dynamodb):
    try:
        table = dynamodb.create_table(
            TableName='ProcessedFiles',
            KeySchema=[
                {
                    'AttributeName': 'Id',  # 고정 파티션 키
                    'KeyType': 'HASH'
                }
            ],
            AttributeDefinitions=[
                {
                    'AttributeName': 'Id',
                    'AttributeType': 'S'
                }
                # ProcessedDate는 여기서 정의할 필요 없음
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 1,
                'WriteCapacityUnits': 1
            }
        )
        # 테이블 생성 대기
        table.wait_until_exists()
        print("Table created successfully.")
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceInUseException':
            print("Table already exists.")
        else:
            raise

def get_last_processed_info(table):
    try:
        response = table.get_item(
            Key={'Id': 'LastProcessedInfo'}
        )
        if 'Item' in response:
            # 파일 이름과 처리 날짜 둘 다 반환
            last_processed_date = datetime.strptime(response['Item']['LastProcessedDate'], '%Y-%m-%dT%H:%M:%S')
            return last_processed_date
        else:
            print("No last processed info found.")
            return None
    except ClientError as e:
        print(f"Failed to fetch last processed info: {e}")
        return None

def update_last_processed_info(table, start_date, last_processed_date):
    try:
        # DynamoDB 항목 업데이트
        response = table.update_item(
            Key={
                'Id': 'LastProcessedInfo'  # 변경: 'LastProcessedDate' -> 'LastProcessedInfo'
            },
            UpdateExpression="SET StartDate = :startdate, LastProcessedDate = :lastprocesseddate",
            ExpressionAttributeValues={
                ':startdate': start_date.isoformat(),
                ':lastprocesseddate': last_processed_date.isoformat()
            },
            ReturnValues="UPDATED_NEW"
        )
        print("Update succeeded:", response)
    except ClientError as e:
        print(f"Failed to update last processed info: {e}")
        
        
def generate_date_paths(start_date, end_date):
    current_date = start_date
    end_date = end_date.replace(hour=23, minute=59, second=59, microsecond=0)
    while current_date <= end_date:
        yield f"{source_path}/{database_name}/{source_table_name}/{current_date.strftime('%Y/%m/%d/')}"
        current_date += timedelta(days=1)


catalog_name = "glue_catalog"
bucket_name = "chiholee-datalake001"
database_name = "ecommerce"

table_name = "orders"
pk = 'order_id'
last_update_time = 'order_dt'

source_bucket_prefix = "transaction/cdc/raw"
source_path = f"s3://{bucket_name}/{source_bucket_prefix}"
source_table_name = table_name

iceberg_bucket_prefix = "transaction/iceberg/emr"
warehouse_path = f"s3://{bucket_name}/{iceberg_bucket_prefix}"
iceberg_table_name = f"{table_name}_cdc_emr_iceberg"


spark = SparkSession.builder \
    .config(f"spark.sql.catalog.{catalog_name}", "org.apache.iceberg.spark.SparkCatalog") \
    .config(f"spark.sql.catalog.{catalog_name}.warehouse", f"{warehouse_path}") \
    .config(f"spark.sql.catalog.{catalog_name}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
    .config(f"spark.sql.catalog.{catalog_name}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
    .config("spark.sql.extensions","org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .getOrCreate()

dynamodb = boto3.resource('dynamodb', region_name="ap-northeast-2")    

try:
    table = dynamodb.Table('ProcessedFiles')
    table.load()
except ClientError as e:
    if e.response['Error']['Code'] == 'ResourceNotFoundException':
        create_table(dynamodb)
        table = dynamodb.Table('ProcessedFiles')
    else:
        raise

last_processed_date = get_last_processed_info(table)
start_date = datetime.now()

print("# last_processed_date : ", last_processed_date)
print("# start_date : ", start_date)

if last_processed_date:
    paths = list(generate_date_paths(last_processed_date, start_date))
else:
    # 마지막 처리 날짜 정보가 없을 경우, 고정 기간을 설정하거나 전체 데이터를 재처리
    paths = list(generate_date_paths(start_date - timedelta(days=30), start_date))

print("# paths : ", paths)

merged_df = None

for path in paths:
    try:
        df = spark.read.option("basePath", f'{source_path}/{database_name}/{source_table_name}/').parquet(path)
        if df.rdd.isEmpty():
            print(f"No data found at {path}")
        else:
            if merged_df is None:
                merged_df = df
                last_processed_file = path  # 첫 번째로 데이터가 있는 파일을 기록
            else:
                merged_df = merged_df.union(df)
            last_processed_file = path  # 최신 파일 경로를 업데이트
    except Exception as e:
        print(f"Error reading data from {path}: {e}")

merged_df.createOrReplaceTempView("cdcDf")

print("# total count : ", merged_df.count())


# 쿼리 작성 로직 조정
if last_processed_date is not None:
    # last_processed_date를 datetime 객체로 변환 (이미 datetime 형태일 수 있음)
    if isinstance(last_processed_date, str):
        last_processed_date = datetime.strptime(last_processed_date, '%Y-%m-%dT%H:%M:%S.%f')
    # last_processed_date에서 1분을 빼기
    time_threshold = last_processed_date - timedelta(minutes=1)
    # 날짜 조건이 있는 쿼리
    query = f"""
    select *
    from cdcDf
    where (order_id, order_dt) in
    (
        select order_id, max(order_dt) max_op_time
        from cdcDf
        WHERE order_dt >= '{time_threshold.strftime('%Y-%m-%d %H:%M:%S')}'
        group by order_id
    )
    """
else:
    # 날짜 조건이 없는 쿼리
    query = """
    select *
    from cdcDf
    where (order_id, order_dt) in
    (
        select order_id, max(order_dt) max_op_time
        from cdcDf
        group by order_id
    )
    """

cdcDf = spark.sql(query)

cdcInsertCount = cdcDf.filter("Op = 'I'").count()
cdcUpdateCount = cdcDf.filter("Op = 'U'").count()
cdcDeleteCount = cdcDf.filter("Op = 'D'").count()
print(f"# Inserted count: {cdcInsertCount}")
print(f"# Updated count: {cdcUpdateCount}")
print(f"# Deleted count: {cdcDeleteCount}")
print(f"# Total CDC count: {cdcDf.count()}")


dropColumnList = ['Op','dms_update_time']


from datetime import datetime
from pyspark.sql.functions import year, month, dayofmonth
from pyspark.sql.functions import concat, col, lit, to_timestamp

current_datetime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
cdcDf = cdcDf.withColumn('order_dt',to_timestamp(col('order_dt')))
cdcDf = (cdcDf
      .withColumn('year', year(col('order_dt')))
      .withColumn('month', month(col('order_dt')))
      .withColumn('day', dayofmonth(col('order_dt')))
     )
cdcDf = cdcDf.withColumn('last_applied_date',to_timestamp(lit(current_datetime)))



upsertDf = cdcDf.filter("Op != 'D'").drop(*dropColumnList)
upsertDf.createOrReplaceTempView(f"{source_table_name}_upsert")


print(f"Table {source_table_name}_iceberg is upserting...")
spark.sql(f"""MERGE INTO {catalog_name}.{database_name}.{iceberg_table_name} t
    USING {source_table_name}_upsert s ON s.{pk} = t.{pk}
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    """)


max_order_dt = upsertDf.agg({"order_dt": "max"}).collect()[0]["max(order_dt)"]

print("# max_order_dt : ", max_order_dt)

update_last_processed_info(table, start_date, max_order_dt)

spark.stop()