#!/bin/python 3
from datetime import date, timedelta, datetime

from sqlalchemy import create_engine

from weather_helpers import *

start = datetime.now()
# connecting to PostgreSQL
engine = create_engine('postgresql://postgres:123456@localhost:5432/weather_data')
connection = engine.connect()

# this code helps me to get all distinct city_ids through which I will iterate
query = "SELECT DISTINCT id FROM public.cities;"
result_proxy = connection.execute(query)
result = result_proxy.fetchall()
cities_list = []
for city in result:
    cities_list.append(city[0])
cities_list.sort()

# I also need to get all distinct years but I will type them manually as there just 5 years used for the analysis
years = [2015, 2016, 2017, 2018, 2019]

# Utilize the create_pandas_table function to create a Pandas data frame
for city_id in cities_list:
    for dist_year in years:
        df = create_pandas_table(
            f"SELECT * FROM observations WHERE city_id = {city_id} AND EXTRACT (YEAR FROM start_time) = {dist_year}",
            engine)
        # Break if no values available for this year
        if (df.size < 1):
            break
        df = clean_df(df)

        # now I need to check for every row if the day whe temperature was measured exist
        start_date = date(dist_year, 1, 1)
        end_date = date(dist_year + 1, 1, 1)
        delta = timedelta(days=1)
        first_day_found = False

        while start_date <= end_date:
            observations_empty = df[df.start_time.dt.date == start_date].empty
            if (observations_empty):
                # By default the last day with observations would be the day before
                last_day_with_observations = start_date - delta
                # First day is empty so we have to look back further
                if first_day_found == False:
                    last_month_df = create_pandas_table(
                        f"SELECT * FROM observations WHERE city_id = {city_id} AND start_time < '{dist_year}-01-01' ORDER BY start_time DESC limit 1000",
                        engine)
                    last_month_df = last_month_df.sort_values(by='start_time', ascending=False)
                    last_day_with_observations = last_month_df.iloc[0].start_time.date()
                    df = df.append(last_month_df)
                newdf = df[df.start_time.dt.date == last_day_with_observations].copy()
                newdf['start_time'] = newdf['start_time'] + (start_date - last_day_with_observations)
                df = df.append(newdf)
            start_date += delta
            first_day_found = True

        # Now that we have all days, we round the times to the closest hour
        df['start_time'] = df['start_time'].dt.round('1h')

        # We start by checking if the first hour of the year is present
        start_time = datetime(dist_year, 1, 1, 0, 0, 0)
        delta_h = timedelta(hours=1)

        observations_empty = df[(df.start_time == start_time)].empty
        if (observations_empty):
            # First hour of the year is empty so we have to look back further, getting the last hour of the previous year
            last_hour_df = create_pandas_table(
                f"SELECT * FROM observations "
                f"WHERE city_id = {city_id} AND start_time < '{dist_year}-01-01' AND temperature IS NOT NULL "
                f"ORDER BY start_time DESC limit 1",
                engine)
            # Set the time of the last hour of previous year to the first hour of this year
            last_hour_df['start_time'] = start_time
            last_hour_df = clean_df(last_hour_df)
            df = df.append(last_hour_df, ignore_index=True)

        # Generate the time range of every hour of this year
        dates_range = pd.date_range(start=f'{dist_year}-01-01 00:00:00', end=f'{dist_year + 1}-01-01 00:00:00',
                                    freq='1h', closed='left')

        # For every hour that is not present in our dataframe, we need to look back for 1 hour and fetch that hour
        # This is guaranteed to work as previously we've made sure that the first hour of the year is present
        for missing_date in dates_range[~dates_range.isin(df['start_time'])]:
            last_date = missing_date - delta_h
            newdf = df[df.start_time == last_date].copy()
            newdf['start_time'] = missing_date
            df = df.append(newdf)

        # Datasets might have duplicate hours
        df = df.drop_duplicates(subset='start_time')
        # Remove previously appended values from last year
        df = df[df['start_time'].dt.year == dist_year]

        # Now I will create an additional column which will have an identificator D as day for hour range 8 am to 10 pm otherwise night
        df = day_night(df)
        # Remove night hours
        df = df[df.day_night == 'D']

        # Now I need to check if there is a rain snow and other unpleasant weather NO else YES
        df = bad_weather(df, engine)

        # Now I need to fill missing is_precipitation values
        df = probability_of_rain_snow(df)
        df = relative_humidity(df)
        df['is_precipitation'] = df['is_precipitation'].astype(bool)
        # Precalculate all the days with precipitation to use in the weather point formula
        hours_with_precipitation = df[df.is_precipitation == True].start_time
        hours_with_precipitation_shifted_forward = hours_with_precipitation.apply(lambda x: x + delta_h)
        hours_with_precipitation_shifted_backward = hours_with_precipitation.apply(lambda x: x - delta_h)
        hours_with_precipitation = hours_with_precipitation.append(hours_with_precipitation_shifted_backward)
        hours_with_precipitation = hours_with_precipitation.append(hours_with_precipitation_shifted_forward)
        hours_with_precipitation = hours_with_precipitation.drop_duplicates()
        hours_with_precipitation = hours_with_precipitation.to_numpy()

        # Now I will create a new column with a point for each day with a use of function f_point_column
        df['Point'] = df.apply(lambda x: calculate_weather_score(x, x.start_time.asm8 in hours_with_precipitation),
                               axis=1)

        # additional cleaning before data are loaded back to postgres
        df = additional_cleaning(df)

        # print out the len of df before uploading and if there are any NULL values in dataframe
        count = df.isnull().sum().sum()
        print(f'Length of dataframe with city_id = {city_id} and year = {dist_year} '
              f'is {len(df)} and {count} null values found')

        # Now save to DB
        to_pg(df, 'observation_analysed', engine)

connection.close()  # stop connection to Database

print(datetime.now() - start, 's')
