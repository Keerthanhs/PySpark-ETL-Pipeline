from pyspark.sql import SparkSession
from pyspark.sql.types import *

def create_spark():

    spark = SparkSession.builder \
        .appName("ETL_Pipeline") \
        .getOrCreate()

    return spark


def define_schema():

    schema = StructType([
        StructField("order_id", IntegerType(), True),
        StructField("customer_id", StringType(), True),
        StructField("product", StringType(), True),
        StructField("amount", IntegerType(), True),
        StructField("city", StringType(), True),
        StructField("date", StringType(), True)
    ])

    return schema


def read_data(spark, path):

    schema = define_schema()

    df = spark.read \
        .option("header", True) \
        .schema(schema) \
        .csv(path)

    return df