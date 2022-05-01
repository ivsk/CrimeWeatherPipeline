class SqlQueries:
    fact_daily_crime_weather_insert = ("""SELECT
					TRUNC(staging_crimes.crime_date) as crime_date,
                    count(staging_crimes.*) as crime_count,
                    SUM(CASE WHEN staging_crimes.arrest IS TRUE THEN 1 ELSE 0 END) as arrest_count,
                    SUM(CASE WHEN staging_crimes.domestic IS TRUE THEN 1 ELSE 0 END) as domestic_count,
                    AVG(staging_weather.temp) as temp,
                    AVG(staging_weather.windspeed) as windspeed,
                    MAX(staging_weather.fog::int) as fog,
                    MAX(staging_weather.rain_drizzle::int) as rain_drizzle,
                    MAX(staging_weather.snow_ice_pellets::int) as snow_ice_pellets,
                    MAX(staging_weather.thunder::int) as thunder
                    FROM staging_crimes
                    LEFT JOIN staging_weather ON TRUNC(staging_crimes.crime_date) = (staging_weather.year::text || '-' || staging_weather.month::text || '-' || staging_weather.day::text)::date
                    GROUP BY TRUNC(staging_crimes.crime_date)
                    ORDER BY TRUNC(staging_crimes.crime_date)
            """)


    dim_table_crime_insert = ("""
            SELECT DISTINCT primary_type 
            FROM staging_crimes
                """)

    dim_table_crime_location_insert = ("""

            SELECT DISTINCT block, community_area, district, ward
            FROM staging_crimes
                """)


    dim_table_crime_arrest_insert = ("""
            SELECT DISTINCT arrest
            FROM staging_crimes
    """)
    dim_table_crime_domestic_insert = ("""
            SELECT DISTINCT domestic
            FROM staging_crimes
        """)

    dim_table_time_insert = ("""
                SELECT crime_date, 
                       extract(hour FROM crime_date), 
                       extract(day FROM crime_date), 
                       extract(week FROM crime_date),
                       extract(year FROM crime_date),
                       extract(dayofweek FROM crime_date)
                FROM staging_crimes
    """)

    dim_table_daily_weather_insert = ("""
                SELECT * 
                FROM staging_weather
    """)