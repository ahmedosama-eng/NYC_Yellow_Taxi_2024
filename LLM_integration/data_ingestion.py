from Insights_Analysis.EDA import aggregate_Taxi
def prepare_for_embadding(df_marged):
    df1=[
    'vendor_name',
    'flag_name',
    'payment_name',
    'rate_name',
    ]
    df2=[
    'DropoffBorough',
    'DropoffZone',
    'Dropoffservice_zone',
    'dropoff_month',
    'dropoff_day',
    'dropoff_day_of_week',
    'dropoff_time_of_day']
    df3=['PickupBorough',
    'PickupZone',
    'Pickupservice_zone',
    'pickup_month',
    'pickup_day',
    'pickup_day_of_week',
    'pickup_time_of_day',]

    df4=['vendor_name',
    'flag_name',
    'payment_name',
    'rate_name',
    'DropoffBorough',
    'DropoffZone',
    'Dropoffservice_zone',
    'dropoff_month',
    'dropoff_day',
    'dropoff_day_of_week',
    'dropoff_time_of_day']

    df5=['vendor_name',
    'flag_name',
    'payment_name',
    'rate_name',
    'PickupBorough',
    'PickupZone',
    'Pickupservice_zone',
    'pickup_month',
    'pickup_day',
    'pickup_day_of_week',
    'pickup_time_of_day',]

    df6=['PickupBorough',
        'PickupZone',
        'Pickupservice_zone',
        'pickup_month',
        'pickup_day',
        'pickup_day_of_week',
        'pickup_time_of_day',
        'DropoffBorough',
        'DropoffZone',
        'Dropoffservice_zone',
        'dropoff_month',
        'dropoff_day',
        'dropoff_day_of_week',
        'dropoff_time_of_day'
        ]




    insight = {
        'Vendor, flag type, rate, and payment type analysis': df1,
        'Drop-off location and time-based trip analysis': df2,
        'Pickup location and time-based trip analysis': df3,
        'Vendor impact on drop-off location and timing': df4,
        'Vendor impact on pickup location and timing': df5,
        'Comprehensive trip analysis (pickup & drop-off)': df6
    }

    aggregatation_piovte_table={}
    for description,dimention in insight.items() :
        pivote_table=aggregate_Taxi(df_marged,dimention)
        aggregatation_piovte_table[description]=pivote_table

    return aggregatation_piovte_table
