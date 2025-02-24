from pyspark.sql import functions as F


def aggregate_Taxi(df, groupby_cols):
    grouped_df = df.groupBy([F.col(col) for col in groupby_cols])
    agg_exprs = [
        F.format_number(F.sum('trip_distance'),2).alias('sum_trip_distance_in_mile'),
        F.format_number(F.avg('trip_distance'), 2).alias('avg_trip_distance_in_mile'),
        F.format_number(F.max('trip_distance'), 2).alias('max_trip_distance_in_mile'),
        F.format_number(F.min('trip_distance'), 2).alias('min_trip_distance_in_mile'),
        F.format_number(F.sum('duration'),2).alias('sum_trip_duration'),
        F.format_number(F.avg('duration'),2).alias('avg_trip_duration'),
        F.format_number(F.max('duration'),2).alias('max_trip_duration'),
        F.format_number(F.min('duration'),2).alias('min_trip_duration'),
        F.format_number(F.sum('fare_amount'), 2).alias('sum_fare_amount'),
        F.format_number(F.avg('fare_amount'), 2).alias('avg_fare_amount'),
        F.format_number(F.max('fare_amount'), 2).alias('max_fare_amount'),
        F.format_number(F.min('fare_amount'), 2).alias('min_fare_amount'),
        F.format_number(F.sum('extra'), 2).alias('sum_extra'),
        F.format_number(F.avg('extra'), 2).alias('avg_extra'),
        F.format_number(F.max('extra'), 2).alias('max_extra'),
        F.format_number(F.min('extra'), 2).alias('min_extra'),
        F.format_number(F.sum('mta_tax'), 2).alias('sum_mta_tax'),
        F.format_number(F.avg('mta_tax'), 2).alias('avg_mta_tax'),
        F.format_number(F.max('mta_tax'), 2).alias('max_mta_tax'),
        F.format_number(F.min('mta_tax'), 2).alias('min_mta_tax'),
        F.format_number(F.sum('tip_amount'), 2).alias('sum_tip_amount'),
        F.format_number(F.avg('tip_amount'), 2).alias('avg_tip_amount'),
        F.format_number(F.max('tip_amount'), 2).alias('max_tip_amount'),
        F.format_number(F.min('tip_amount'), 2).alias('min_tip_amount'),
        F.format_number(F.sum('improvement_surcharge'), 2).alias('sum_improvement_surcharge'),
        F.format_number(F.avg('improvement_surcharge'), 2).alias('avg_improvement_surcharge'),
        F.format_number(F.max('improvement_surcharge'), 2).alias('max_improvement_surcharge'),
        F.format_number(F.min('improvement_surcharge'), 2).alias('min_improvement_surcharge'),
        F.format_number(F.sum('total_amount'), 2).alias('sum_total_amount'),
        F.format_number(F.avg('total_amount'), 2).alias('avg_total_amount'),
        F.format_number(F.max('total_amount'), 2).alias('max_total_amount'),
        F.format_number(F.min('total_amount'), 2).alias('min_total_amount'),
        F.format_number(F.sum('congestion_surcharge'), 2).alias('sum_congestion_surcharge'),
        F.format_number(F.avg('congestion_surcharge'), 2).alias('avg_congestion_surcharge'),
        F.format_number(F.max('congestion_surcharge'), 2).alias('max_congestion_surcharge'),
        F.format_number(F.min('congestion_surcharge'), 2).alias('min_congestion_surcharge'),
        F.format_number(F.sum('Airport_fee'), 2).alias('sum_Airport_fee'),
        F.format_number(F.avg('Airport_fee'), 2).alias('avg_Airport_fee'),
        F.format_number(F.max('Airport_fee'), 2).alias('max_Airport_fee'),
        F.format_number(F.min('Airport_fee'), 2).alias('min_Airport_fee'),
        F.count('Airport_fee').alias('Trip_counts')
    ]
    result_df = grouped_df.agg(*agg_exprs)
    return result_df
    
