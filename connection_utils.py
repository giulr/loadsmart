#!/usr/bin/env python
# coding: utf-8

# In[16]:


import pandas.io.sql as psql
import psycopg2 as pg


# In[18]:


def connect():
     return pg.connect("host=localhost dbname=dw user=postgres password=postgres")
    
def check_table_existence(cursor, table_name):
    cursor.execute("select * from information_schema.tables where table_name=%s", (table_name,))
    return bool(cursor.rowcount)

def generate_columns_list(df):
    return ", ".join([str(i) for i in df.columns.tolist()])

def insert(df, connection, table_name):
    cols = generate_columns_list(df)

    for i,row in df.iterrows():
        insert_script = "INSERT INTO " + table_name +   "(" +cols + ") VALUES (" + "%s,"*(len(row)-1) + "%s)"
        on_duplicate = "ON CONFLICT ("+cols+" )  DO NOTHING"
        connection.cursor().execute(insert_script, tuple(row))
    
    connection.commit()

