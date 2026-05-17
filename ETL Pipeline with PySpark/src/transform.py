from pyspark.sql.functions import *

def clean_data(df):

    cleaned_df = (

        df.dropna(
            subset=["customer_id"]
        )

        .filter(
            col("amount") > 0
        )

        .fillna(
            {"city":"Unknown"}
        )

        .dropna(
            subset=["date"]
        )
    )

    return cleaned_df


def transform_data(df):

    transformed_df = (

        df.withColumn(
            "date",
            to_date(col("date"))
        )

        .withColumn(
            "tax",
            col("amount")*0.18
        )

        .withColumn(
            "final_amount",
            col("amount")+
            col("amount")*0.18
        )
    )

    return transformed_df


def incremental_load(new_df, existing_df):

    max_id = existing_df.agg(
        max("order_id")
    ).collect()[0][0]

    filtered_df = new_df.filter(
        col("order_id") > max_id
    )

    return filtered_df