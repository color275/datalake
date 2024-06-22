#  zip -r project.zip *.py utils/

from pyspark.sql import SparkSession
import utils.read_glue_catalog as glue_catalog

if __name__ == '__main__':
    # PySpark 세션 생성
    spark = SparkSession.builder \
        .appName("Glue Catalog Read Example") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
        .config("spark.jars.packages",
                ###########################################################
                # s3 를 read 하기 위한 필수 jars
                # java.lang.ClassNotFoundException: Class org.apache.hadoop.fs.s3a.S3AFileSystem not found
                ###########################################################
                "org.apache.hadoop:hadoop-aws:3.3.4"
                ###########################################################
                ) \
        .getOrCreate()

    # Glue Catalog 에서 테이블 정보 조회
    region_name = 'ap-northeast-2'
    database_name = 'ecommerce'
    table_name = 'cr_order'
    table_info = glue_catalog.get_table_from_glue(database_name, table_name, region_name)

    # 동적 스키마 생성
    schema = glue_catalog.create_schema(table_info)

    # 테이블 위치에서 데이터를 읽어와서 DataFrame으로 변환
    s3_location = table_info['StorageDescriptor']['Location']

    # s3:// 형식을 s3a:// 형식으로 변경
    if s3_location.startswith("s3://"):
        s3_location = s3_location.replace("s3://", "s3a://", 1)

    # 데이터 읽기 (파티션 키 포함)
    df = spark.read \
        .schema(schema) \
        .option("basePath", s3_location) \
        .parquet(s3_location)

    # 데이터 프레임 조회
    df.show(10)
