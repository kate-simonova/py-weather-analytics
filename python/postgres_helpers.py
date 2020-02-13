#!/bin/python 3
import pandas as pd
import io

# creating a pandas dataframe from sql query (borrowed from medium)
def create_pandas_table(sql_query, database):
    df = pd.read_sql_query(sql_query, database)
    return df

# Now I need to upload analysed dataset back to postgres
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
