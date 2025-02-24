from Transform.data_transformer import Transform_Cleaning_and_Marge_DWH
from Transform.modling import return_data_diminstion,preprocess_diminstions

from load.database_connection import connect_to_database
from load.data_loader import load_into_DataWarhouse




def pipline_for_DWH(data,lookups,spark):
    fact_table=Transform_Cleaning_and_Marge_DWH(data,spark)
    Dimensions=preprocess_diminstions(lookups,spark)
    date_dim=return_data_diminstion("2024-01-01","2024-12-31")
    jdbc_url,db_properties=connect_to_database()
    load_into_DataWarhouse(fact_table,jdbc_url,db_properties,'Taxi_facts')
    load_into_DataWarhouse(Dimensions['Store_and_fwd_flag'],jdbc_url,db_properties,'Store_and_fwd_flag_dim')
    load_into_DataWarhouse(Dimensions['payment_type_lookup'],jdbc_url,db_properties,'payment_type_dim')
    load_into_DataWarhouse(Dimensions['rate_lookup'],jdbc_url,db_properties,'rate_dim')
    load_into_DataWarhouse(Dimensions['taxi_zone_lookup'],jdbc_url,db_properties,'taxi_zone_dim')
    load_into_DataWarhouse(Dimensions['vendor_lookup'],jdbc_url,db_properties,'vendor_dim')
    load_into_DataWarhouse(date_dim,jdbc_url,db_properties,'date_dim')

