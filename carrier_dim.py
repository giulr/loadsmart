#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import connection_utils as conn
import pandas.io.sql as psql


# In[3]:


def execute():
    connection = conn.connect()

    df_carrier = psql.read_sql(''' SELECT DISTINCT carrier_name, vip_carrier FROM raw_data_challenge''', con=connection)
    if not conn.check_table_existence(connection.cursor(), 'carrier_dim'):
        create_dim_carrier = ('''
                CREATE TABLE public.carrier_dim
            (
                carrier_dim_id serial NOT NULL,
                carrier_name character varying(500),
                vip_carrier boolean,
                PRIMARY KEY (carrier_dim_id)
            );

            ALTER TABLE public.carrier_dim
                OWNER to postgres;
        ''')
        connection.cursor().execute(create_dim_carrier)
        connection.commit()
        print('carrier dimension table created successfully')
    else:
        print('carrier dimension table has already been created')

    conn.insert(df_carrier, connection, 'carrier_dim')

    connection.close()

