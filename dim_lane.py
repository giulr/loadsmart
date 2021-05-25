#!/usr/bin/env python
# coding: utf-8

# In[11]:


import pandas as pd
import connection_utils as conn
import pandas.io.sql as psql


# In[13]:


def execute():
    connection = conn.connect()

    df_lane = psql.read_sql(''' SELECT DISTINCT lane FROM raw_data_challenge''', con=connection)
    if not conn.check_table_existence(connection.cursor(), 'lane_dim'):
        create_dim_carrier = ('''
                       CREATE TABLE public.lane_dim
                        (
                            lane_dim_id serial NOT NULL,
                            lane character varying(500),
                            PRIMARY KEY (lane_dim_id)
                        );

                        ALTER TABLE public.lane_dim
                            OWNER to postgres;
        ''')
        connection.cursor().execute(create_dim_carrier)
        connection.commit()
        print('lane dimension table created successfully')
    else:
        print('lane dimension table has already been created')

    conn.insert(df_lane, connection, 'lane_dim')

    connection.close()

