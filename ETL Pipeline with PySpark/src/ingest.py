from pyspark.sql import SparkSession
from pyspark.sql.types import *

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
