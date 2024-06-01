from pyspark.sql import SparkSession
from datetime import datetime
from datetime import datetime, timedelta
import os

os.environ['AWS_ACCESS_KEY_ID'] = '***'
os.environ['AWS_SECRET_ACCESS_KEY'] = '***'
os.environ['AWS_DEFAULT_REGION'] = 'ap-northeast-2'



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

    # .config("spark.sql.extensions","org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
spark = SparkSession.builder \
    .enableHiveSupport() \
    .config(f"enable_glue_datacatalog", "") \
    .config(f"spark.sql.catalog.{catalog_name}", "org.apache.iceberg.spark.SparkCatalog") \
    .config(f"spark.sql.catalog.{catalog_name}.warehouse", f"{warehouse_path}") \
    .config(f"spark.sql.catalog.{catalog_name}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
    .config(f"spark.sql.catalog.{catalog_name}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
    .getOrCreate()


# 테이블 조회를 위한 SQL 쿼리 실행
df = spark.sql(f"SELECT * FROM ecommerce.accesslog")

# # DataFrame 내용 출력
# df.show()