from pyspark.sql.functions import *
from pyspark.sql.window import Window


def revenue_by_city(df):

    result = (

        df.groupBy("city")

        .agg(
            sum("final_amount")
            .alias(
                "total_revenue"
            )
        )
    )

    return result


def customer_rank(df):

    windowSpec = Window.partitionBy(
        "city"
    ).orderBy(
        col("final_amount").desc()
    )

    ranked_df = df.withColumn(
        "rank",
        dense_rank()
        .over(windowSpec)
    )

    return ranked_df