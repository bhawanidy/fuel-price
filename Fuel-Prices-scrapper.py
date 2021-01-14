#!/usr/bin/env python
# coding: utf-8

# In[1]:


import os
import requests 
import numpy as np
import pandas as pd
import json
from tqdm import tqdm
from datetime import datetime


# In[2]:


cities = ["new-delhi", "kolkata", "bangalore", "chennai", "mumbai", "hyderabad" ]


# In[3]:


daily_data = [] 
monthly_data = []

for city in tqdm(cities):
    
    print(f"Now processing for {city}")
    url = f"https://www.goodreturns.in/src/fuels.php?cmd=fuel_rates_4_graph&city={city}&fuel_type=diesel&callback=jQuery&_=1"
    print(f"Using the url : {url}")
    
    response = requests.get(url)
    
    if int(response.status_code) != 200:
        print(f"Something went wrong. HTTP Resp Code : {response.status_code}")
              
    raw_text = response.text.strip()
    prices_list_str = raw_text[7:-1]  # keep charcters from 8th pos (0-based indexing) till last char ( excluding it )
    
    j = json.loads(prices_list_str) 
    
    df = pd.DataFrame(j)
    df.drop(columns=["x","y"], inplace=True)  # remove unwanted columns
    df["price_date"] = pd.to_datetime(df["price_date"])  # convert column to pandas date-time object
    df.set_index(keys=["price_date"], inplace=True)
    df.columns = [city]
    daily_data.append(df)   
    
    monthly_df = df.groupby(by=pd.Grouper(freq="M")).mean()
    monthly_data.append(monthly_df)


# In[28]:


len(daily_data), len(monthly_data)


# In[30]:


daily_data[5].tail(3)


# In[32]:


monthly_data[5].tail(3)


# In[33]:


daily_combined = daily_data[0]
for df in daily_data[1:]:
    daily_combined = daily_combined.join(df, how="inner")
daily_combined.tail(5)    


# In[41]:


monthly_combined = monthly_data[0]
for df in monthly_data[1:]:
    monthly_combined = monthly_combined.join(df, how="inner")
    
monthly_combined["National Average (Metros)"] = monthly_combined[["new-delhi", "kolkata", "chennai", "mumbai"] ].mean(axis=1)
monthly_combined.tail(5) 


# In[42]:


now = str(datetime.now())[:10]
print(now)


# In[44]:


daily_file_name = f"daily_fuel_prices_in_metros_{now}.xlsx"
monthly_file_name = f"monthly_fuel_prices_in_metros_{now}.xlsx"


# In[45]:


daily_combined.to_excel(daily_file_name)
monthly_combined.to_excel(monthly_file_name)


# In[ ]:




