#!/usr/bin/env python
# coding: utf-8

# In[6]:


import pandas as pd
import connection_utils as conn
import pandas.io.sql as psql


# In[11]:


def execute():

    connection = conn.connect()

    df_shipper = psql.read_sql(''' SELECT DISTINCT shipper_name FROM raw_data_challenge''', con=connection)
    if not conn.check_table_existence(connection.cursor(), 'shipper_dim'):
        create_dim_carrier = ('''
                       CREATE TABLE public.shipper_dim
                        (
                            shipper_dim_id serial NOT NULL,
                            shipper_name character varying(500),
                            PRIMARY KEY (shipper_dim_id)
                        );

                        ALTER TABLE public.shipper_dim
                            OWNER to postgres;
        ''')
        connection.cursor().execute(create_dim_carrier)
        connection.commit()
        print('fact table created successfully')
    else:
        print('fact table has already been created')

    conn.insert(df_shipper, connection, 'shipper_dim')

    connection.close()

