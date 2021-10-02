# -*- coding: utf-8 -*-
"""
Created on Thu Sep 30 14:34:37 2021

@author: nvbr
"""

import pandas as pd


covid_confirmed=pd.read_csv("time_series_covid_19_confirmed_US.csv")

#print(covid_confirmed.columns[11:len(covid_confirmed.columns)])


x1 = covid_confirmed.melt(id_vars=covid_confirmed.columns[1:10], 
                          value_vars=covid_confirmed.columns[11:len(covid_confirmed.columns)])

grp=x1[['Province_State','Admin2','variable','value']].groupby(['Province_State', 'Admin2','variable']).sum()
grp.reset_index(level=0, inplace=True)
grp['date']=grp.index.get_level_values('variable')

grp['city']=grp.index.get_level_values('Admin2')

grp.reset_index(drop=True, inplace=True)

#grp['month'] =pd.to_datetime(grp['date']).dt.month
grp['Quarter'] = pd.cut(pd.to_datetime(grp['date']).dt.month, bins=[0,4,7,10,12], labels=['Q1','Q2','Q3','Q4'])
grp['Quarter'] = grp['Quarter'].astype(str)+pd.to_datetime(grp['date']).dt.year.astype(str)

#grp['Quarter']=get_quarter(grp['date'])
#Test Sreehitha