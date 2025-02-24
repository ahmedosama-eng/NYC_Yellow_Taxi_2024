

def load_into_DataWarhouse(df,jdbc_url,db_properties,table_name,spark):
    df.write \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", f"public.{table_name}") \
    .option("user", db_properties["user"]) \
    .option("password", db_properties["password"]) \
    .option("driver", db_properties["driver"]) \
    .mode("overwrite") \
    .save()
