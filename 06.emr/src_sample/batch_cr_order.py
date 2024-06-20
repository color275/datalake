# spark-submit --deploy-mode client --master yarn orders_cdc_upsert_iceberg.py
# spark-submit --deploy-mode cluster --master yarn orders_cdc_upsert_iceberg.py
# spark-submit --deploy-mode cluster --master yarn \
# --num-executors 1 \
# --executor-cores 2 \
# --executor-memory 2g \
# orders_cdc_upsert_iceberg.py

from pyspark.sql import SparkSession
from pyspark.sql import SparkSession
from datetime import datetime
from datetime import datetime, timedelta
from botocore.exceptions import ClientError

catalog_name = "glue_catalog"
bucket_name = "chiholee-datalake001"
iceberg_bucket_prefix = "transaction/iceberg/emr"
warehouse_path = f"s3://{bucket_name}/{iceberg_bucket_prefix}"

spark = SparkSession.builder \
    .config(f"spark.sql.catalog.{catalog_name}", "org.apache.iceberg.spark.SparkCatalog") \
    .config(f"spark.sql.catalog.{catalog_name}.warehouse", f"{warehouse_path}") \
    .config(f"spark.sql.catalog.{catalog_name}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
    .config(f"spark.sql.catalog.{catalog_name}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
    .config("spark.sql.extensions","org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .getOrCreate()

sql = """
SELECT
    p.product_id,
    p.name,
    COALESCE(ra.view_count, 0) AS view_count,
    COALESCE(ro.order_count, 0) AS order_count,
    CASE 
        WHEN COALESCE(ra.view_count, 0) > 0 
        THEN ROUND(COALESCE(ro.order_count, 0) / CAST(COALESCE(ra.view_count, 0) AS DOUBLE) * 100, 2)
        ELSE 0
    END AS conversion_rate,
    ro.date AS order_date,
    substr(ro.date, 1, 4) AS year,
    substr(ro.date, 5, 2) AS month,
    substr(ro.date, 7, 2) AS day
    
FROM 
    glue_catalog.ecommerce.product_cdc_glue_iceberg p
LEFT JOIN (
    SELECT 
        product_id,
        date_format(ts, 'yyyyMMdd') AS date,
        COUNT(*) AS view_count
    FROM 
        ecommerce.accesslog
    GROUP BY 
        product_id,
        date_format(ts, 'yyyyMMdd')
) ra ON p.product_id = ra.product_id
LEFT JOIN (
    SELECT 
        product_id,
        date_format(order_dt, 'yyyyMMdd') AS date,
        COUNT(*) AS order_count
    FROM 
        glue_catalog.ecommerce.orders_cdc_emr_iceberg
    GROUP BY 
        product_id,
        date_format(order_dt, 'yyyyMMdd')
) ro ON p.product_id = ro.product_id AND ra.date = ro.date
ORDER BY 
    order_date desc, conversion_rate DESC
"""

df_cr_order = spark.sql(sql)
df_cr_order = df_cr_order.repartition("year", "month", "day").sortWithinPartitions("year", "month", "day")
s3_path = "s3://chiholee-datalake001/cr_order/"
df_cr_order.write.partitionBy("year", "month", "day") \
    .parquet(s3_path, mode="overwrite")

spark.stop()