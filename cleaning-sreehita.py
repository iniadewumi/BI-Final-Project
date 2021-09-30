# -*- coding: utf-8 -*-
"""
Created on Thu Sep 30 14:34:37 2021

@author: nvbr
"""

import io
import pandas as pd
import requests as r
import numpy as np

covid_confirmed=pd.read_csv("time_series_covid_19_confirmed_US.csv")

print(covid_confirmed.columns[11:len(covid_confirmed.columns)])


x1 = covid_confirmed.melt(id_vars=covid_confirmed.columns[1:10], 
                          value_vars=covid_confirmed.columns[11:len(covid_confirmed.columns)])

grp=x1[['Province_State','Admin2','variable','value']].groupby(['Province_State','variable']).sum()

grp.reset_index(level=0, inplace=True)

grp['date']=grp.index

def get_quarter(month):
    quarter_dictionary = {
        "Q1" : [1,2,3],
        "Q2" : [4,5,6],
        "Q3" : [7,8,9],
        "Q4" : [10,11,12]
    }

    
   
    # for key,values in quarter_dictionary.items():
    #     for value in values:
    #         if value == pd.to_datetime(grp['date']).dt.month:
    #            return cat(key,pd.to_datetime(grp['date']).dt.year.astype(str))

#grp['month']=pd.to_datetime(grp['date']).dt.month

grp['Quarter']=grp.apply(lambda x:get_quarter(x['date']),axis=1)#  get_quarter(grp['month'])

#grp['Quarter']=get_quarter(grp['date'])

grp['Quarter'] = grp['Quarter']+pd.to_datetime(grp['date']).dt.year

#grp['Quarter']=get_quarter(grp['date'])