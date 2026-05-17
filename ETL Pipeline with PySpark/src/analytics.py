from pyspark.sql.functions import *

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
