#!/usr/bin/env python
# coding: utf-8

# In[14]:


import pandas as pd
import connection_utils as conn
import pandas.io.sql as psql


# In[16]:


def execute():

    connection = conn.connect()

    df_tracking = psql.read_sql(''' SELECT DISTINCT has_mobile_app_tracking, has_macropoint_tracking, has_edi_tracking
    FROM raw_data_challenge''', con=connection)

    if not conn.check_table_existence(connection.cursor(), 'tracking_dim'):
        create_dim_carrier = ('''
                       CREATE TABLE public.tracking_dim
                        (
                            tracking_dim_id serial NOT NULL,
                            has_mobile_app_tracking boolean,
                            has_macropoint_tracking boolean,
                            has_edi_tracking boolean,
                            PRIMARY KEY (tracking_dim_id)
                        );

                        ALTER TABLE public.tracking_dim
                            OWNER to postgres;
        ''')
        connection.cursor().execute(create_dim_carrier)
        connection.commit()
        print('tracking dimension table created successfully')
    else:
        print('tracking dimension table has already been created')

    conn.insert(df_tracking, connection, 'tracking_dim')

    connection.close()

