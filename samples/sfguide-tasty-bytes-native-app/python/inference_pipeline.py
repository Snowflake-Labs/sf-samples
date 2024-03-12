import snowflake.snowpark.functions as F
import snowflake.snowpark.types as T
from snowflake.snowpark import Session

def inference(session: Session, city: T.StringType, shift: T.IntegerType, forecast_date: T.DateType):
    # create dimmensional table for date and shifts
    dates_df = session.create_dataframe(
        [[forecast_date.month, forecast_date.weekday(), shift]], 
        schema = ["month", "day_of_week", "shift"]
    )

    # pull locations information from provider
    locations_df = session.sql('''
                        select * 
                        from package_shared.location_detail_v
                        ''')
    locations_df = locations_df.select([
        "city", 
        "location_name", 
        "location_id", 
        "count_locations_within_half_mile", 
        "latitude", "longitude", 
        "city_population",
    ])
    locations_df = locations_df.filter(F.col("city") == city)
    
    # cross join with dates to have all locations and date/shift combinations
    locations_df = locations_df.cross_join(dates_df)
    
    # get sales data from consumer
    sales_df = session.sql('''
                        select location_id, shift_sales 
                        from reference('shift_sales')
                        ''')
    sales_df = sales_df.group_by("location_id").agg(F.mean("shift_sales").alias("avg_location_shift_sales"))
    
    # join sales and location data for inference
    locations_df = locations_df.join(
        sales_df, locations_df.location_id == sales_df.location_id
    ).rename(
        locations_df.location_id, "location_id"
    ).drop(sales_df.location_id)

    # run inference by calling evaluate_model udf
    features = [
        'MONTH', 
        'DAY_OF_WEEK', 
        'LATITUDE', 
        'LONGITUDE', 
        'COUNT_LOCATIONS_WITHIN_HALF_MILE', 
        'CITY_POPULATION', 
        'AVG_LOCATION_SHIFT_SALES', 
        'SHIFT',
    ]
    df_results =  locations_df.select(
        F.col("city"),
        F.col("location_name"),
        F.col("location_id"),
        F.col("latitude"),
        F.col("longitude"),
        F.col("month"),
        F.col("day_of_week"),
        F.iff(F.col("shift") == 1, "AM", "PM").alias("shift"),
        F.call_udf("code_schema.evaluate_model", [F.col(c) for c in features]).alias("prediction")
    ).order_by(
        F.col("prediction").desc()
    ).limit(10)
    
    return df_results
