#!/bin/python 3
from meteocalc import Temp, heat_index
from numpy import random

from postgres_helpers import *

# Caching for the precipitation text values
weather_df = None


# this function is used for cleaning df namely for droping unnecessary columns and duplicates
def clean_df(dataframe):
    columns_to_delete = ["id", "end_time", "day_ind", "airport_four_letter_code", "dew_point", "wx_icon",
                         "pressure", "visibility", "feels_like", "uv_index", "wind_speed"]
    dataframe = dataframe.drop(columns_to_delete, axis=1)  # here I will delete all unnecessary columns from df
    dataframe = dataframe.drop_duplicates()  # just in case if there are some duplicates
    dataframe = dataframe.dropna(subset=['temperature'])  # Drop rows with null temperature
    dataframe.location = dataframe.location.fillna(
        method='ffill')  # if location is missing for a certain city_id propagate last valid observation forward
    # For rows where temperature is equal to zero, we fill the blank heat indexes with 0 to normalize the dataset
    dataframe[dataframe.temperature == 0] = dataframe[dataframe.temperature == 0].fillna({'heat_index': 0})
    # If heat_index is null we calculate heat_index manually, but first we should convert temperature to fahrenheit
    # dataframe = dataframe.replace('nan', "")
    dataframe = relative_humidity(dataframe)
    dataframe['heat_index'] = dataframe.apply(calculate_heat_index, axis=1)
    return dataframe


# this function is used to create a column which will show if it is a night either day time
def day_night(dataframe):
    dataframe['hour'] = dataframe['start_time'].dt.hour
    dataframe['day_night'] = dataframe.hour.apply(lambda x: 'D' if x in range(8, 22) else 'N')
    dataframe = dataframe.drop('hour', axis=1)
    return dataframe


# this function calls a new table created in Postgres where all rainy and snowy days have boolean True others are marked as False
def bad_weather(dataframe, engine):
    global weather_df
    if weather_df is None:
        # for this I will create a new table in postgres with basic commands written in word
        weather_df = create_pandas_table('select wx_phrase, is_precipitation from distinct_wx_phrase;', engine)
        weather_df = weather_df.set_index('wx_phrase')  # Set the index to make it possible to map values directly
    dataframe['is_precipitation'] = dataframe['wx_phrase'].map(weather_df['is_precipitation'])
    return dataframe


# this function creates a new column which calculates a point for every weather condition
def calculate_weather_score(row, rainy_hour):
    weather_score = 1.0
    heat_index = int(row['heat_index'])
    # Penalize rainy days
    if (rainy_hour):
        weather_score = 0
    if heat_index > 24:
        weather_score = weather_score * pow(0.9, heat_index - 24)
    if heat_index < 18:
        weather_score = weather_score * pow(0.9, 18 - heat_index)
    return round(weather_score, 3)


# this function will be called to calculate relative humidity where it is missing
def relative_humidity(dataframe):
    dataframe.relative_humidity = dataframe.relative_humidity.fillna(method='ffill')
    # in case the very first row is empty
    dataframe.relative_humidity = dataframe.relative_humidity.fillna(method='bfill')
    # If we still have missing values (e.g. if only one datapoint is provided), we default to a hardcoded value
    dataframe.relative_humidity = dataframe.relative_humidity.fillna(value=75)
    return dataframe


# this function is called when heat index is null so we need to calculate it based on temperature and relative humidity
def calculate_heat_index(row):
    t = Temp(int(row['temperature']), 'c')
    rh = int(row['relative_humidity'])
    hi = heat_index(temperature=t, humidity=rh)
    return round(hi.c)


# this function will calculate a probability of a rain or snow as we have missing some missing wx_phrase
def probability_of_rain_snow(dataframe):
    odds_of_rain = dataframe.is_precipitation.sum() / dataframe.is_precipitation.size
    dataframe.is_precipitation = dataframe.is_precipitation.fillna(
        lambda x: True if random.random() < odds_of_rain else False)
    return dataframe


# this function will be called to remove all unnecessary columns created or used for analysis
def additional_cleaning(dataframe):
    cols_to_delete = ['temperature', 'wx_phrase', 'relative_humidity']
    dataframe = dataframe.drop(cols_to_delete, axis=1)
    dataframe.reset_index(inplace=True, drop=True)
    return dataframe
