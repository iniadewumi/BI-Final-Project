# -*- coding: utf-8 -*-
"""
Created on Thu Sep 30 14:34:37 2021

@author: nvbr
"""

import pandas as pd
import requests, io


source = {
    'confirmed': 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_US.csv',
    'death': 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_US.csv'
    }

class DataUpdater:
    def _init_(self):
        pass
    
    def get_csv(self, data, dataframe:bool=True):
        response = requests.get(source[data])
        if response.status_code == 200:
            if dataframe:
                return self.get_dataframe(response)
            return response
        
        raise f"Failed to get request! {response.status_code}"
    
    def get_dataframe(self, response):
        return pd.read_csv(io.StringIO(response.text))
    
    
data = DataUpdater()
death = data.get_csv('death')
confirmed = data.get_csv('confirmed')



def intoquarterly(df1,k):

    x1 = df1.melt(id_vars=df1.columns[1:k-1],value_vars=df1.columns[k:len(df1.columns)])

    x1.reset_index(level=0, inplace=True)
    #x1['date']=x1.index.get_level_values('variable')
    
    #x1.rename(columns={"variable": "date", "value": "confirmed_cases_cnt"})
    
    print(x1.columns)
#grp['city']=grp.index.get_level_values('Admin2')

    x1.reset_index(drop=True, inplace=True)
    
    x1['Quarter'] = pd.cut(pd.to_datetime(x1['variable']).dt.month, bins=[0,4,7,10,12], labels=['Q1','Q2','Q3','Q4'])
    x1['Quarter'] = x1['Quarter'].astype(str)+pd.to_datetime(x1['variable']).dt.year.astype(str)


    #grp=x1[['Province_State','Admin2','variable','value']].groupby(['Province_State', 'Admin2','variable']).sum()
    grp=x1[['Province_State','Quarter','value']].groupby(['Province_State', 'Quarter']).sum()

    # grp.reset_index(level=0, inplace=True)
    # grp['date']=grp.index.get_level_values('variable')

    # #grp['city']=grp.index.get_level_values('Admin2')

    # grp.reset_index(drop=True, inplace=True)

    # #grp['month'] =pd.to_datetime(grp['date']).dt.month
    
#grp['Quarter']=get_quarter(grp['date'])
    return(grp)

df_confirmed=intoquarterly(confirmed,11)

df_death=intoquarterly(death,12)

df_confirmed.to_csv("confirmed.csv")

df_death.to_csv("deaths_df.csv")
