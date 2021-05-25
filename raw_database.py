#!/usr/bin/env python
# coding: utf-8

# In[167]:


import pandas as pd
import connection_utils as conn
import pandas.io.sql as psql
from sqlalchemy import create_engine


# In[173]:


def execute():
    connection = conn.connect()

    if not conn.check_table_existence(connection.cursor(), 'raw_data_challenge'):
        create_raw_table = '''
                            CREATE TABLE public.raw_data_challenge
                            (
                                loadsmart_id bigint NOT NULL,
                                lane character varying(500),
                                quote_date character varying(200),
                                book_date character varying(200),
                                source_date character varying(200),
                                pickup_date character varying(200),
                                delivery_date character varying(200),
                                book_price character varying(200),
                                source_price character varying(200),
                                pnl character varying(200),
                                mileage character varying(200),
                                equipment_type character varying(3),
                                carrier_rating character varying(200),
                                sourcing_channel character varying(100),
                                vip_carrier character varying(200),
                                carrier_dropped_us_count character varying(200),
                                carrier_name character varying(200),
                                shipper_name character varying(200),
                                carrier_on_time_to_pickup character varying(200),
                                carrier_on_time_to_delivery character varying(200),
                                carrier_on_time_overall character varying(200),
                                pickup_appointment_time character varying(200),
                                delivery_appointment_time character varying(200),
                                has_mobile_app_tracking character varying(200),
                                has_macropoint_tracking character varying(200),
                                has_edi_tracking character varying(200),
                                contracted_load character varying(200),
                                load_booked_autonomously character varying(200),
                                load_sourced_autonomously character varying(200),
                                load_was_cancelled character varying(200),
                                PRIMARY KEY (loadsmart_id)
                            );

                            ALTER TABLE public.raw_data_challenge
                                OWNER to postgres;
                           '''
        connection.cursor().execute(create_raw_table)
        connection.commit()
        print('raw table created successfully')
    else:
        print('raw table has already been created')

    df_raw_data = pd.read_csv('database.csv')                  .drop(['has_mobile_app_tracking.1'], axis=1)

    cols = conn.generate_columns_list(df_raw_data)

    for i,row in df_raw_data.iterrows():
        insert_script = "INSERT INTO raw_data_challenge (" +cols + ") VALUES (" + "%s,"*(len(row)-1) + "%s)"
        ignore_duplicate = " ON CONFLICT(loadsmart_id)  DO NOTHING"
        connection.cursor().execute(insert_script+ignore_duplicate, tuple(row))


    connection.commit()

    connection.close()

