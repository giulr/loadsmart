#!/usr/bin/env python
# coding: utf-8

# In[6]:


import raw_database as raw
import fact_table as fact
import dim_shipper as dim_shipper
import dim_lane as dim_lane
import dim_load as dim_load
import dim_tracking as dim_tracking
import carrier_dim as dim_carrier
import connection_utils as conn


# In[8]:


raw.execute() #Raw database Creation

dim_shipper.execute() #shipper dimension creation

dim_lane.execute() #lane dimension creation

dim_load.execute() #load dimension creation

dim_carrier.execute() #carrier dimension creation

dim_tracking.execute() #tracking dimension creation

fact.execute() #fact table creation

