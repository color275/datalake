from awsglue.job import Job
catalog_name = "glue_catalog"
bucket_name = "chiholee-datalake001"
database_name = "ecommerce"

table_name = "orders"
pk = 'order_id'
last_update_time = 'order_dt'

source_bucket_prefix = "transaction/cdc/raw"
source_path = f"s3://{bucket_name}/{source_bucket_prefix}"
source_table_name = table_name

iceberg_bucket_prefix = "transaction/iceberg/glue"
warehouse_path = f"s3://{bucket_name}/{iceberg_bucket_prefix}"
iceberg_table_name = f"{table_name}_cdc_glue_iceberg"
from pyspark.sql import SparkSession
spark = SparkSession.builder \
    .config(f"spark.sql.catalog.{catalog_name}", "org.apache.iceberg.spark.SparkCatalog") \
    .config(f"spark.sql.catalog.{catalog_name}.warehouse", f"{warehouse_path}") \
    .config(f"spark.sql.catalog.{catalog_name}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
    .config(f"spark.sql.catalog.{catalog_name}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
    .config("spark.sql.extensions","org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .getOrCreate()
import sys
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions


glueContext = GlueContext(spark)

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

cdcDyf = glueContext.create_dynamic_frame_from_options(
    connection_type='s3',
    connection_options={
        'paths': [f'{source_path}/{database_name}/{source_table_name}/'],
        'groupFiles': 'none',
        'recurse': True
    },
    format='parquet',
    transformation_ctx='cdcDyf')

print(f"## Count of CDC data after last job bookmark:{cdcDyf.count()}")
cdcDf = cdcDyf.toDF()
cdcDf.show()
import sys
from pyspark.sql import Window
from pyspark.sql import functions as F 
cdcDf.createOrReplaceTempView("cdcDf")
cdcDf = spark.sql("""
select *
from cdcDf
where (order_id, order_dt) in
(
    select order_id, max(order_dt) max_op_time
    from cdcDf
    group by order_id
)
"""
)

cdcInsertCount = cdcDf.filter("Op = 'I'").count()
cdcUpdateCount = cdcDf.filter("Op = 'U'").count()
cdcDeleteCount = cdcDf.filter("Op = 'D'").count()

print(f"Inserted count: {cdcInsertCount}")
print(f"Updated count: {cdcUpdateCount}")
print(f"Deleted count: {cdcDeleteCount}")
print(f"Total CDC count: {cdcDf.count()}")
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


spark.sql(f"CREATE DATABASE IF NOT EXISTS {catalog_name}.{database_name}")
existing_tables = spark.sql(f"SHOW TABLES IN {catalog_name}.{database_name};")
df_existing_tables = existing_tables.select('tableName').rdd.flatMap(lambda x:x).collect()
upsertDf = cdcDf.filter("Op != 'D'").drop(*dropColumnList)
upsertDf.createOrReplaceTempView(f"{source_table_name}_upsert")
upsertDf.show()

# spark.sql(f"""
# select order_id, count(*)
# from {catalog_name}.{database_name}.{iceberg_table_name}
# group by order_id
# having count(*) > 1
# """).show()
# spark.sql(f"""
# select order_id, count(*)
# from {source_table_name}_upsert
# group by order_id
# having count(*) > 1
# """).show()

deleteDf = cdcDf.filter("Op = 'D'").drop(*dropColumnList)
deleteDf.createOrReplaceTempView(f"{source_table_name}_delete")

# deleteDf.show()
print(f"Table {source_table_name}_iceberg is upserting...")
spark.sql(f"""MERGE INTO {catalog_name}.{database_name}.{iceberg_table_name} t
    USING {source_table_name}_upsert s ON s.{pk} = t.{pk}
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    """)

spark.sql(f"""
select min(order_dt), max(order_dt)
from {catalog_name}.{database_name}.{iceberg_table_name}
""").show()

job.commit()