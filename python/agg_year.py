#!/bin/python 3
from sqlalchemy import create_engine
import pandas as pd
import io
import time

start_time = time.time()
# connecting to PostgreSQL
engine = create_engine('postgresql://postgres:123456@localhost:5432/weather2')
connection = engine.connect()

# this code helps me to get all distinct city_ids through which I will iterate
query = "SELECT DISTINCT city_id FROM public.observations;"
result_proxy = connection.execute(query)
result = result_proxy.fetchall()
cities_list = []
for city in result:
    cities_list.append(city[0])
cities_list.sort()

# creating a pandas dataframe from sql query
def create_pandas_table(sql_query, database=engine):
    df = pd.read_sql_query(sql_query, database)
    return df

# this function is used for cleaning df namely for droping unnecessary columns
def clean_df(dataframe):
    columns_to_remove = ['day_night', 'is_precipitation', 'heat_index']
    dataframe = dataframe.drop(columns_to_remove, axis=1)  # here I will delete all unnecessary columns from df
    return dataframe

#this function is used for extracting year for aggregation
def extract_year(dataframe):
    dataframe['year'] = dataframe['start_time'].dt.year
    return dataframe

# this function is used for aggregation and calculation of year score
def aggregation_by_year(dataframe):
    dataframe = dataframe.groupby(['city_id','year']).mean()*100
    return dataframe

# this two functions are used to load analysed data back to postgres
def cleanColumns(columns):
    cols = []
    for col in columns:
        col = col.replace(' ', '_')
        cols.append(col)
    return cols

def to_pg(df, table_name, con):
    data = io.StringIO()
    df.columns = cleanColumns(df.columns)
    df.to_csv(data, header=False, index=False)
    data.seek(0)
    raw = con.raw_connection()
    curs = raw.cursor()
    empty_table = pd.io.sql.get_schema(df, table_name, con=con)
    empty_table = empty_table.replace('"', '')
    empty_table = empty_table.replace('CREATE TABLE', 'CREATE TABLE IF NOT EXISTS')
    curs.execute(empty_table)
    curs.copy_from(data, table_name, sep=',')
    curs.connection.commit()

# Utilize the create_pandas_table function to create a Pandas data frame
for i in cities_list:
    df = create_pandas_table(f"SELECT * FROM observation_analysed WHERE city_id = {i}")
    df = extract_year(df)
    df = clean_df(df)
    df = aggregation_by_year(df)
    df.reset_index(inplace=True, drop=False) # in order to keep all necessary information

    # print out the len of df before uploading and if there are any NULL values in dataframe
    print(f'City_id = {i} is grouped')
    # Now save to DB
    to_pg(df, 'agg_year', con=engine)

#print(df)

connection.close()


print(time.time() - start_time, 's')