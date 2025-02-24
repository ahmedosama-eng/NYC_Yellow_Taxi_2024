from pyspark.sql.functions import col,split, year, month, dayofmonth, hour, minute, when, date_format,unix_timestamp,count,round,concat_ws,lit\
,explode, sequence, to_date, expr, year, month, dayofmonth, date_format

from pyspark.sql import Row

def Transform_Cleaning_and_Marge_LLM(data,lookups,spark):
        taxi_zone_pickup = lookups['taxi_zone_lookup'].alias("pickup_zone")
        
        taxi_zone_dropoff = lookups['taxi_zone_lookup'].alias("dropoff_zone")
        c=0
        for i in data.keys():
                df=data[i].join(lookups['rate_lookup'],lookups['rate_lookup'].rate_id==data[i].RatecodeID,'left')\
                                .join(lookups['payment_type_lookup'],lookups['payment_type_lookup'].payment_id==data[i].payment_type,'left')\
                                .join(taxi_zone_pickup,col('PULocationID')==col("pickup_zone.LocationID"),'left')\
                                .join(taxi_zone_dropoff,col('DOLocationID')==col("dropoff_zone.LocationID"),'left')\
                                .join(lookups['vendor_lookup'],lookups['vendor_lookup'].vendor_id==data[i].VendorID,'left')\
                                .join(lookups['Store_and_fwd_flag'],lookups['Store_and_fwd_flag'].flag==data[i].store_and_fwd_flag,'left')\
                                .withColumn('tpep_pickup_datetime',col('tpep_pickup_datetime').cast('timestamp'))\
                                .withColumn('tpep_dropoff_datetime',col('tpep_dropoff_datetime').cast('timestamp'))\
                                .withColumn("duration",round((unix_timestamp(col("tpep_dropoff_datetime")) - unix_timestamp(col("tpep_pickup_datetime"))) / 60,2))\
                                .withColumn("pickup_month", month("tpep_pickup_datetime")) \
                                .withColumn("pickup_day", dayofmonth("tpep_pickup_datetime")) \
                                .withColumn("pickup_day_of_week", date_format("tpep_pickup_datetime", "EEEE"))\
                                .withColumn("pickup_hour", hour("tpep_pickup_datetime")) \
                                .withColumn("pickup_minute", minute("tpep_pickup_datetime")) \
                                .withColumn("pickup_time_of_day", 
                                        when(col("pickup_hour").between(0, 5), "Morning")
                                        .when(col("pickup_hour").between(6, 11), "Afternoon")
                                        .when(col("pickup_hour").between(12, 17), "Evening")
                                        .when(col("pickup_hour").between(18, 23), "Night")
                                        )\
                                .withColumn("dropoff_month", month("tpep_dropoff_datetime")) \
                                .withColumn("dropoff_day", dayofmonth("tpep_dropoff_datetime")) \
                                .withColumn("dropoff_day_of_week", date_format("tpep_dropoff_datetime", "EEEE"))\
                                .withColumn("dropoff_hour", hour("tpep_dropoff_datetime")) \
                                .withColumn("dropoff_minute", minute("tpep_dropoff_datetime")) \
                                .withColumn("dropoff_time_of_day", 
                                        when(col("dropoff_hour").between(0, 5), "Morning")
                                        .when(col("dropoff_hour").between(6, 11), "Afternoon")
                                        .when(col("dropoff_hour").between(12, 17), "Evening")
                                        .when(col("dropoff_hour").between(18, 23), "Night")
                                        )
                if c==0:
                        df_marged=df
                        c=1
                        continue
                
                df_marged=df_marged.union(df)


        df_marged=df_marged.select(
                        col('vendor_name'),
                        col('flag_name'),
                        col('pickup_zone.Borough').alias("PickupBorough"),
                        col('pickup_zone.Zone').alias("PickupZone"),
                        col('pickup_zone.service_zone').alias("Pickupservice_zone"),
                        col('dropoff_zone.Borough').alias("DropoffBorough"),
                        col('dropoff_zone.Zone').alias("DropoffZone"),
                        col('dropoff_zone.service_zone').alias("Dropoffservice_zone"),
                        col('payment_name'),
                        col('rate_name'),
                        col('tpep_pickup_datetime'),
                        col('pickup_month'),
                        col('pickup_day'),
                        col('pickup_day_of_week'),
                        col('pickup_hour'),
                        col('pickup_minute'),
                        col('pickup_time_of_day'),
                        col('tpep_dropoff_datetime'),
                        col('dropoff_month'),
                        col('dropoff_day'),
                        col('dropoff_day_of_week'),
                        col('dropoff_hour'),
                        col('dropoff_minute'),
                        col('dropoff_time_of_day'),
                        col('duration'),
                        col('passenger_count'),
                        col('trip_distance'),
                        col('fare_amount'),
                        col('extra'),
                        col('mta_tax'),
                        col('tip_amount'),
                        col('improvement_surcharge'),
                        col('total_amount'),
                        col('congestion_surcharge'),
                        col('Airport_fee')
                )

        df_marged=df_marged.fillna({"passenger_count": 0})\
                        .fillna({"congestion_surcharge": 0})\
                        .fillna({"Airport_fee": 0})\
                        .fillna({"flag_name": "unknown"})\
                        .fillna({"payment_name":  "unknown"})\
                        .fillna({"rate_name": "unknown"})
        
        return df_marged




def Transform_Cleaning_and_Marge_DWH(data,spark):
        c=0
        for i in data.keys():
                df=data[i].withColumn('tpep_pickup_datetime',col('tpep_pickup_datetime').cast('timestamp'))\
                                .withColumn('tpep_dropoff_datetime',col('tpep_dropoff_datetime').cast('timestamp'))\
                                .withColumn("pickup_date", split(col("tpep_pickup_datetime")," ")[0])\
                                .withColumn("pickup_time", split(col("tpep_pickup_datetime")," ")[1])\
                                .withColumn("dropoff_date", split(col("tpep_dropoff_datetime")," ")[0])\
                                .withColumn("dropoff_time", split(col("tpep_dropoff_datetime")," ")[1])\
                                .withColumn("pickup_date_key", date_format(col("pickup_date"), "yyyyMMdd").cast("int"))\
                                .withColumn("dropoff_date_key", date_format(col("dropoff_date"), "yyyyMMdd").cast("int"))\
                                .withColumn("duration",round((unix_timestamp(col("tpep_dropoff_datetime")) - unix_timestamp(col("tpep_pickup_datetime"))) / 60,2))\
                                .withColumn("pickup_hour", hour("tpep_pickup_datetime")) \
                                .withColumn("pickup_time_of_day", 
                                        when(col("pickup_hour").between(0, 5), "Morning")
                                        .when(col("pickup_hour").between(6, 11), "Afternoon")
                                        .when(col("pickup_hour").between(12, 17), "Evening")
                                        .when(col("pickup_hour").between(18, 23), "Night")
                                        )\
                                .withColumn("dropoff_hour", hour("tpep_dropoff_datetime")) \
                                .withColumn("dropoff_time_of_day", 
                                        when(col("dropoff_hour").between(0, 5), "Morning")
                                        .when(col("dropoff_hour").between(6, 11), "Afternoon")
                                        .when(col("dropoff_hour").between(12, 17), "Evening")
                                        .when(col("dropoff_hour").between(18, 23), "Night")
                                        )
                if c==0:
                        df_marged_without=df
                        c=1
                        continue
                
                df_marged_without=df_marged_without.union(df)


        df_marged_without=df_marged_without.select(
                
                        col('VendorID'),
                        col('pickup_date_key'),
                        col('pickup_time'),
                        col('dropoff_date_key'),
                        col('dropoff_time'),
                        col('trip_distance'),
                        col('RatecodeID'),
                        col('store_and_fwd_flag'),
                        col('PULocationID'),
                        col('DOLocationID'),
                        col('payment_type'),               
                        col('duration'),
                        col('passenger_count'),
                        col('fare_amount'),
                        col('extra'),
                        col('mta_tax'),
                        col('tip_amount'),
                        col('improvement_surcharge'),
                        col('total_amount'),
                        col('congestion_surcharge'),
                        col('Airport_fee')
                )

        df_marged_without=df_marged_without.fillna({"passenger_count": 0})\
                        .fillna({"RatecodeID": 0})\
                        .fillna({"store_and_fwd_flag": 0})\
                        .fillna({"congestion_surcharge": 0})\
                        .fillna({"Airport_fee": 0})\
                  


        return df_marged_without



