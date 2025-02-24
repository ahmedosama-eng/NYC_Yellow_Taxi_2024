from load.database_connection import connect_to_database
def reading_structured_data(jdbc_url,db_properties,spark):
    Taxi_df = spark.read \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "public.taxi_2024")\
    .option("user", db_properties["user"]) \
    .option("password", db_properties["password"]) \
    .option("driver", db_properties["driver"]) \
    .load()

    return Taxi_df



def reading_data(table_name,jdbc_url,db_properties,spark):
    df = spark.read \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", f"public.{table_name}")\
    .option("user", db_properties["user"]) \
    .option("password", db_properties["password"]) \
    .option("driver", db_properties["driver"]) \
    .load()

    return df

