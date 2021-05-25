#!/usr/bin/env python
# coding: utf-8

# In[8]:


import pandas as pd
import connection_utils as conn
import pandas.io.sql as psql


# In[10]:


def execute():
    connection = conn.connect()

    df_load = psql.read_sql(''' SELECT DISTINCT load_booked_autonomously, 
    load_sourced_autonomously, load_was_cancelled, contracted_load  FROM raw_data_challenge''', con=connection)

    if not conn.check_table_existence(connection.cursor(), 'load_dim'):
        create_dim_carrier = ('''
                       CREATE TABLE public.load_dim
                        (
                            load_dim_id serial NOT NULL,
                            load_booked_autonomously boolean,
                            load_sourced_autonomously boolean,
                            load_was_cancelled boolean,
                            contracted_load boolean,
                            PRIMARY KEY (load_dim_id)
                        );

                        ALTER TABLE public.load_dim
                            OWNER to postgres;
        ''')
        connection.cursor().execute(create_dim_carrier)
        connection.commit()
        print('load dimension table created successfully')
    else:
        print('load dimension table has already been created')

    conn.insert(df_load, connection, 'load_dim')
    connection.close()

