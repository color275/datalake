import os
from pyspark.sql import SparkSession

# AWS 자격 증명 설정 (환경 변수 사용)
os.environ['AWS_ACCESS_KEY_ID'] = '***'
os.environ['AWS_SECRET_ACCESS_KEY'] = '***'
os.environ['AWS_DEFAULT_REGION'] = 'ap-northeast-2'

# SparkSession 설정
spark = SparkSession.builder \
    .appName("Glue Table Access") \
    .config("spark.sql.catalogImplementation", "hive") \
    .config("hive.metastore.glue.datacatalog.enabled", "true") \
    .config("spark.hadoop.hive.metastore.client.factory.class", "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory") \
    .config("spark.jars.packages", "com.amazonaws:aws-java-sdk-bundle:1.11.271,org.apache.hadoop:hadoop-aws:2.7.3") \
    .enableHiveSupport() \
    .getOrCreate()

# AWS Glue 테이블의 정보
database_name = "your_database_name"
table_name = "your_table_name"

# 테이블 조회를 위한 SQL 쿼리 실행
df = spark.sql(f"SELECT * FROM {database_name}.{table_name}")

# DataFrame 내용 출력
df.show()
