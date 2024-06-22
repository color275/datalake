from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, IntegerType, StringType, StructType, TimestampType, LongType
from pyspark.sql.functions import dayofmonth, month, year

if __name__ == '__main__':
    env = "staging"
    catalog_name = "glue_catalog"
    bucket_name = "chiholee-datalake001"
    soruce_database_name = f"ecommerce"
    target_database_name = f"{env}_ecommerce"

    table_name = "orders"
    last_update_time = 'order_dt'

    source_bucket_prefix = "transaction/initial/raw"
    source_path = f"s3a://{bucket_name}/{source_bucket_prefix}"

    iceberg_bucket_prefix = f"{env}/transaction/"
    warehouse_path = f"s3a://{bucket_name}/{iceberg_bucket_prefix}"


    spark = SparkSession.builder \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
        .config(f"spark.sql.catalog.{catalog_name}", "org.apache.iceberg.spark.SparkCatalog") \
        .config(f"spark.sql.catalog.{catalog_name}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
        .config(f"spark.sql.catalog.{catalog_name}.warehouse", f"{warehouse_path}") \
        .config("spark.jars.packages",
                ###########################################################
                ## iceberg 를 read 하기 위한 필수 jars
                ###########################################################
                "org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.5.2,"
                "software.amazon.awssdk:bundle:2.17.230,"
                ###########################################################
                ###########################################################
                # s3 를 read 하기 위한 필수 jars
                # java.lang.ClassNotFoundException: Class org.apache.hadoop.fs.s3a.S3AFileSystem not found
                ###########################################################
                "org.apache.hadoop:hadoop-aws:3.3.4,"
                # "org.apache.iceberg:iceberg-spark-extensions-3.4_2.12:1.5.2,"
                # "software.amazon.awssdk:url-connection-client:2.17.230,"
                # "org.apache.hadoop:hadoop-aws:3.3.4,"
                # "com.amazonaws:aws-java-sdk-bundle:1.11.901,"
                # "com.amazonaws:aws-java-sdk-core:1.12.725,"
                # "com.amazona  ws:jmespath-java:1.12.725"
                ) \
        .appName("Iceberg Full") \
        .getOrCreate()

    schema = StructType([
        StructField("order_id", IntegerType(), True),
        StructField("promo_id", StringType(), True),
        StructField("order_cnt", IntegerType(), True),
        StructField("order_price", IntegerType(), True),
        StructField("order_dt", TimestampType(), True),
        StructField("customer_id", LongType(), True),  # INT64 이므로 LongType 으로 선언
        StructField("product_id", IntegerType(), True)
    ])

    staging_df = spark.read \
                .schema(schema) \
                .parquet(f'{source_path}/{soruce_database_name}/{table_name}/')

    staging_df.createOrReplaceTempView(table_name)

    range_df = spark.sql(f"""
                            select  *,
                                    year(order_dt) as year,
                                    month(order_dt) as month,
                                    dayofmonth(order_dt) as day            
                            from {table_name}                                
                        """)

    part_df = range_df.repartition('year', 'month', 'day') \
                .sortWithinPartitions("year", "month", "day")

    print("# df count : ", part_df.count())

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {catalog_name}.{target_database_name}")

    spark.sql(f"SHOW DATABASES IN {catalog_name}").show()

    part_df.createOrReplaceTempView(table_name)

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {catalog_name}.{target_database_name}.{table_name}
        USING iceberg
        PARTITIONED BY (year, month, day)
        AS (
            SELECT *
            FROM {table_name}
        )
    """)

    spark.stop()




