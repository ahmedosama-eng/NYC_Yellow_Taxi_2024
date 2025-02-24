
from pyspark.sql.functions import col, year, month, dayofmonth, date_format,explode, sequence, to_date, expr, year, month, dayofmonth
from pyspark.sql import Row

def return_data_diminstion(start_date,end_date,spark): 
        
        
        df = spark.createDataFrame([(start_date, end_date)], ["start_date", "end_date"]) \
        .withColumn("start_date", to_date(col("start_date"))) \
        .withColumn("end_date", to_date(col("end_date"))) \
        .withColumn("date", explode(sequence(col("start_date"), col("end_date"), expr("INTERVAL 1 DAY"))))

        
        df = df.withColumn("date_key", date_format(col("date"), "yyyyMMdd").cast("int"))\
        .withColumn("year", year(col("date"))) \
        .withColumn("month", month(col("date"))) \
        .withColumn("day", dayofmonth(col("date"))) \
        .withColumn("day_of_week", date_format(col("date"), "EEEE")) \
        
        return df



def preprocess_diminstions(lookups,spark):
       new_row = Row(0, "unkown")
       new_df = spark.createDataFrame([new_row], lookups['payment_type_lookup'].schema)
       lookups['payment_type_lookup']=lookups['payment_type_lookup'].union(new_df)
 
       new_row = Row("None", "unkown")
       new_df = spark.createDataFrame([new_row], lookups['Store_and_fwd_flag'].schema)
       lookups['Store_and_fwd_flag']=lookups['Store_and_fwd_flag'].union(new_df)

       
       new_row = Row(0, "unkown")
       new_df = spark.createDataFrame([new_row], lookups['rate_lookup'].schema)
       lookups['rate_lookup']=lookups['rate_lookup'].union(new_df)

       return lookups