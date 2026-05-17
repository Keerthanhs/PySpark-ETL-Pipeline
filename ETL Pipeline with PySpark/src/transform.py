from pyspark.sql.functions import *

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
