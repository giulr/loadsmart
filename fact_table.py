#!/usr/bin/env python
# coding: utf-8

# In[23]:


import pandas as pd
import connection_utils as conn
import pandas.io.sql as psql


# In[25]:


def execute():
    connection = conn.connect()

    query = '''
    SELECT 
        loadsmart_id, TO_TIMESTAMP(pickup_date, 'MM/DD/YYYY HH24:MI')::timestamp without time zone AS pickup_date,
        TO_TIMESTAMP(delivery_date, 'MM/DD/YYYY HH24:MI')::timestamp without time zone AS delivery_date,
        book_price, source_price, pnl, mileage, equipment_type, carrier_rating, sourcing_channel,
        carrier_dropped_us_count, carrier_on_time_to_pickup, carrier_on_time_to_delivery, carrier_on_time_overall,
        TO_TIMESTAMP(pickup_appointment_time, 'MM/DD/YYYY HH24:MI')::timestamp without time zone pickup_appointment_time, 
        TO_TIMESTAMP(delivery_appointment_time, 'MM/DD/YYYY HH24:MI')::timestamp without time zone delivery_appointment_time,
        l.lane_dim_id, s.shipper_dim_id, t.tracking_dim_id, lo.load_dim_id, c.carrier_dim_id,
        1 AS "count"
    FROM 
    raw_data_challenge raw
    INNER JOIN lane_dim l ON (raw.lane = l.lane)
    INNER JOIN shipper_dim s ON (raw.shipper_name = s.shipper_name)
    INNER JOIN tracking_dim t ON (raw.has_mobile_app_tracking::boolean = t.has_mobile_app_tracking AND
        raw.has_macropoint_tracking::boolean = t.has_macropoint_tracking AND
        raw.has_edi_tracking::boolean = t.has_edi_tracking
    )
    INNER JOIN load_dim lo ON (raw.load_booked_autonomously::boolean = lo.load_booked_autonomously AND
        raw.load_sourced_autonomously::boolean = lo.load_sourced_autonomously AND
        raw.load_was_cancelled::boolean = lo.load_was_cancelled AND 
        raw.contracted_load::boolean = lo.contracted_load 
    )
    INNER JOIN carrier_dim c ON  (raw.carrier_name = c.carrier_name AND
        raw.vip_carrier::boolean = c.vip_carrier
    )

    '''

    df_fact = psql.read_sql(query, con=connection)

    df_fact['delivery_days_diff'] = (pd.to_datetime(df_fact['delivery_date'])-pd.to_datetime(df_fact['pickup_date'])).dt.days

    if not conn.check_table_existence(connection.cursor(), 'fact_table'):
        create_fact_table = ('''
                       CREATE TABLE public.fact_table
                (
                    fact_table_id serial NOT NULL,
                    loadsmart_id bigint,
                    pickup_date timestamp without time zone,
                    delivery_date timestamp without time zone,
                    book_price double precision,
                    source_price double precision,
                    pnl double precision,
                    mileage double precision,
                    equipment_type character varying(3),
                    carrier_rating character varying,
                    sourcing_channel character varying,
                    carrier_dropped_us_count integer,
                    carrier_on_time_to_pickup character varying,
                    carrier_on_time_to_delivery character varying,
                    carrier_on_time_overall character varying,
                    pickup_appointment_time timestamp without time zone,
                    delivery_appointment_time timestamp without time zone,
                    lane_dim_id integer,
                    shipper_dim_id integer,
                    tracking_dim_id integer,
                    load_dim_id integer,
                    carrier_dim_id integer,
                    count integer,
                    delivery_days_diff integer,
                    PRIMARY KEY (fact_table_id)
                );

                ALTER TABLE public.fact_table
                    OWNER to postgres;
        ''')
        connection.cursor().execute(create_fact_table)
        connection.commit()
        print('fact table created successfully')
    else:
        print('fact table has already been created')

    conn.insert(df_fact, connection, 'fact_table')

    connection.close()

