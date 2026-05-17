# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession

# COMMAND ----------

spark = SparkSession.builder \
    .appName("ETL_Pipeline") \
    .getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC #### DATA READING (BRONZE LAYER)

# COMMAND ----------



# COMMAND ----------

schema = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("customer_id", StringType(), True),
    StructField("product", StringType(), True),
    StructField("amount", IntegerType(), True),
    StructField("city", StringType(), True),
    StructField("date", StringType(), True)
])

# COMMAND ----------

df = spark.read \
    .format("csv") \
    .option("header",True) \
    .schema(schema) \
    .load("/Volumes/workspace/default/datasets/orders_large.csv")

# COMMAND ----------

df.display()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC #### DATA CLEANING (SILVER LAYER)
# MAGIC

# COMMAND ----------

df=df.dropna(subset=["customer_id"])

# COMMAND ----------

df=df.filter(df.amount>0)

# COMMAND ----------

df=df.fillna({
    "city":"Unknown"
})

# COMMAND ----------

df=df.dropna(subset=["date"])

# COMMAND ----------

# MAGIC %md
# MAGIC #### DATA TRANSFORMATION (SILVER LAYER)

# COMMAND ----------

df=df.withColumn(
    "date",
    to_date(col("date"))
)

# COMMAND ----------

df=df.withColumn(
    "tax",
    col("amount")*0.18
)

# COMMAND ----------

df=df.withColumn(
    "final_amount",
    col("amount")+col("tax")
)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC #### DATA ANALYTICS (GOLD LAYER)
# MAGIC

# COMMAND ----------

result=df.groupBy("city")\
         .agg(
            sum("final_amount")
            .alias("total_sales")
         )

result.show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### DATA WRITING

# COMMAND ----------

# DBTITLE 1,Cell 21
df.write \
    .mode("overwrite") \
    .partitionBy("city") \
    .parquet("/Volumes/workspace/default/datasets/orders_partitioned")

# COMMAND ----------

