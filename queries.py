# redefine dataframe keeping only needed columns, 
# splitting the timestamp and keeping the date, 
# rounding for readability, and nulling fields for values out of 
# reasonable ranges (atmospheric pressure in Pa and atmospheric temperature in C)
query_clean = """
    select
        sensor_id,
        substring(timestamp,1,instr(timestamp,'T')-1) as date,
        case 
            when temperature>100 or temperature<-100 then null
            else round(temperature,2)
            end as temperature,
        case 
            when pressure>121000 or pressure<5600 then null
            else round(pressure,2)
            end as pressure
    from
        sensor_read
"""

# first query result to df, get average temperature per sensor
# the aggregate function ignores nulls, coalesce avoids null fields and gives a cleaner and more 
# explicit indication when temperature not available for the sensor
# Column becomes non numeric, this has to be removed in case of subsequent processing with numbers
query_1 = """
    select
        sensor_id,
        coalesce(round(avg(temperature),2),'N/A') as avg_temp
    from
        sensor_read
    group by
        1
"""

# second query result to df, get min and max temp and pressure per sensor and day
# as for previous query, aggregation function ignore nulls, 
# and coalesce is used to explicit unavailable measurements. 
# To be removed if column is needed to be kept numeric 
query_2 = """
    select
        sensor_id,
        date,
        coalesce(max(temperature),'N/A') as max_temp,
        coalesce(min(temperature),'N/A') as min_temp,
        coalesce(max(pressure),'N/A') as max_press,
        coalesce(min(pressure),'N/A') as min_press
    from 
        sensor_read
    group by
        1,2
"""
