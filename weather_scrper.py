#!/usr/bin/env python3
'''
this script is used to scrape the data from a popular weather website
'''

# loading necessary modules
import urllib.request
import pandas as pd
import random
from sqlalchemy import create_engine
from datetime import date, timedelta
import time

# Postgres_helpers
# Functions used for loading data to postgres
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

user_agent_list = [
    # Chrome
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.113 Safari/537.36',
    'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.90 Safari/537.36',
    'Mozilla/5.0 (Windows NT 5.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.90 Safari/537.36',
    'Mozilla/5.0 (Windows NT 6.2; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.90 Safari/537.36',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/44.0.2403.157 Safari/537.36',
    'Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.113 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/57.0.2987.133 Safari/537.36',

    # Firefox
    'Mozilla/4.0 (compatible; MSIE 9.0; Windows NT 6.1)',
    'Mozilla/5.0 (Windows NT 6.1; WOW64; Trident/7.0; rv:11.0) like Gecko',
    'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0)',
    'Mozilla/5.0 (Windows NT 6.1; Trident/7.0; rv:11.0) like Gecko',
    'Mozilla/5.0 (Windows NT 6.2; WOW64; Trident/7.0; rv:11.0) like Gecko',
    'Mozilla/5.0 (Windows NT 10.0; WOW64; Trident/7.0; rv:11.0) like Gecko',
    'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.0; Trident/5.0)',
    'Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 5.1; Trident/4.0; .NET CLR 2.0.50727; .NET CLR 3.0.4506.2152; .NET CLR 3.5.30729)'
]

# connecting to PostgreSQL
engine = create_engine('postgresql://postgres:123456@localhost:5432/lastvdb')
connection = engine.connect()

# this code helps me to get all distinct airports_codes and two letter codes through which I will iterate
query = "SELECT airport_code, code FROM public.codes;"
result_proxy = connection.execute(query)
result = result_proxy.fetchall()
l1, l2 = [], []
for airport in result:
    l1.append(airport[0])
    l2.append(airport[1])

airport_list = list(zip(l1, l2))

# create empty dataframe to append data
main_df = pd.DataFrame(columns=["airport", 'obs_name', "temp", 'wx_phrase', 'dewPoint', 'heat_index',
                                'relative_humidity', 'pressure', 'valid_time_gmt', 'current_day'])

# create datetime to iterate the whole 5 years
d1 = date(2015, 1, 1)
d2 = date(2015, 12, 31)
delta = timedelta(days=1)

for airport in airport_list:
    user_agent = random.choice(user_agent_list)
    while d1 <= d2:
        current_date = str(d1)
        current_date = current_date.replace("-", "")

        # Set the headers
        headers = {'User-Agent': user_agent}
        td = []

        url = f"WEATHER_WEBSITE_startDate={current_date}&endDate={current_date}"
        # Make the request
        request = urllib.request.Request(url,
                                         headers=headers)
        response = urllib.request.urlopen(request)
        rawpage = response.read().decode("utf-8")
        the_page = rawpage.replace('<!---->', '')
        list = the_page.split(',')

        airports, temperatures, wx_phrase, dewPt, heat_index, rh, pressure, obs_name, valid_time_gmt = [], [], [], [], [], [], [], [], []

        for i in list:
            if i.startswith('"obs_id"'):
                airports.append(i[10:])
            if i.startswith('"obs_name"'):
                obs_name.append(i[12:])
            if i.startswith('"valid_time_gmt"'):
                valid_time_gmt.append(time.gmtime(int(i[17:])))
            if i.startswith('"temp"'):
                temperatures.append(i[7:])
            if i.startswith('"wx_phrase"'):
                wx_phrase.append(i[12:])
            if i.startswith('"dewPt"'):
                dewPt.append(i[8:])
            if i.startswith('"heat_index"'):
                heat_index.append(i[13:])
            if i.startswith('"rh"'):
                rh.append(i[5:])
            if i.startswith('"pressure"'):
                pressure.append(i[12:])

        # print(len(airports), len(temperatures), len(wx_phrase), len(dewPt), len(heat_index), len(rh), len(pressure), len(obs_name), len(valid_time_gmt))

        df = pd.DataFrame({"airport": airports,
                           'obs_name': obs_name,
                           "temp": temperatures,
                           'wx_phrase': wx_phrase,
                           'dewPoint': dewPt,
                           'heat_index': heat_index,
                           'relative_humidity': rh,
                           'pressure': pressure,
                           'valid_time_gmt': valid_time_gmt})

        df['current_day'] = d1

        main_df = main_df.append(df, ignore_index=True)
        print(main_df)

        print(airport, current_date)
        d1 += delta
    time.sleep(0.3)

#main_df.to_csv('all_data')
# to_pg(main_df, 'my_data', engine)
