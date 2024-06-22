from pyspark.sql import SparkSession
from pyspark.sql import SparkSession
import boto3
from datetime import datetime
from datetime import datetime, timedelta
from botocore.exceptions import ClientError

if __name__ == "__main__":
    catalog_name = "glue_catalog"
    database_name = "ecommerce"
    table_name = "orders_cdc_emr_iceberg"

    spark = SparkSession.builder \
        .config(f"spark.sql.catalog.{catalog_name}", "org.apache.iceberg.spark.SparkCatalog") \
        .config(f"spark.sql.catalog.{catalog_name}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
        .config("spark.jars.packages",
                ###########################################################
                ## iceberg 를 read 하기 위한 필수 jars
                ###########################################################
                "org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.5.2,"
                "software.amazon.awssdk:bundle:2.17.230,"
                ###########################################################
                # "org.apache.iceberg:iceberg-spark-extensions-3.4_2.12:1.5.2,"
                # "software.amazon.awssdk:url-connection-client:2.17.230,"
                # "org.apache.hadoop:hadoop-aws:3.3.4,"
                # "com.amazonaws:aws-java-sdk-bundle:1.11.901,"
                # "com.amazonaws:aws-java-sdk-core:1.12.725,"
                # "com.amazonaws:jmespath-java:1.12.725"
                ) \
        .appName("Iceberg Read Example") \
        .getOrCreate()

    spark.sql(f"""select * from {catalog_name}.{database_name}.{table_name}""").show()
