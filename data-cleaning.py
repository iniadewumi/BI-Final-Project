from ProjectData import GoogleSheets, DataUpdater
import pandas as pd
import numpy as np
import requests
import io, os
import math

sheets = GoogleSheets()
data = sheets.dataframes()
df_names = [f'{k + 1}. {v}' for k,v in enumerate(data.keys())]

print("\nBelow are the available keys to get each dataframe:")
print("\n".join(df_names))


gdp = data.get('gdp_sheet_df')
hospital = data.get('hospital_ratings_df')
death = data.get('death')
confirmed = data.get('confirmed')
state = data.get('state')


def normal_round(n):
    try:
        return math.floor(n) if n - math.floor(n) < 0.5 else math.ceil(n)
    except ValueError:
        return np.nan

def convert_hospital_ratings_to_int():    
    print("\nConverting Hospital Procedure Quality to Integers")
    hospital[['Procedure_Heart_Attack_Quality',
           'Procedure_Heart_Attack_Value', 'Procedure_Heart_Failure_Quality',
           'Procedure_Pneumonia_Quality', 'Procedure_Hip_Knee_Quality',
           'Procedure_Hip_Knee_Value', 'Procedure_Pneumonia_Value',
           'Procedure_Heart_Failure_Value']] = hospital[[ 'Procedure_Heart_Attack_Quality',
           'Procedure_Heart_Attack_Value', 'Procedure_Heart_Failure_Quality',
           'Procedure_Pneumonia_Quality', 'Procedure_Hip_Knee_Quality',
           'Procedure_Hip_Knee_Value', 'Procedure_Pneumonia_Value',
           'Procedure_Heart_Failure_Value']].replace({'Higher':3, 'Average':2, 'Lower':1, 'Unknown':np.nan}).replace({'Better':3, 'Worse':1})

    print("Converting Hospital Ratings to Integers")    
    hospital[['Rating_Mortality', 'Rating_Safety',
           'Rating_Readmission', 'Rating_Experience', 'Rating_Effectiveness',
           'Rating_Timeliness', 'Rating_Imaging']] = hospital[['Rating_Mortality', 'Rating_Safety',
           'Rating_Readmission', 'Rating_Experience', 'Rating_Effectiveness',
           'Rating_Timeliness', 'Rating_Imaging']].replace({'Below':0, 'Same':1, 'None':np.nan, 'Above':2})
    hospital['Rating_Overall'] = hospital['Rating_Overall'].astype(float)
    hospital['Rating_Overall'] = hospital['Rating_Overall'].replace(-1, np.nan)

    # print(hospital.dtypes)

def convert_gdp_to_float():
    quarters = [x for x in gdp.columns if "Q" in x]
    gdp[quarters] = gdp[quarters].astype(str).replace('[\$,]', '', regex=True).astype(float)
    gdp.columns = [col.replace(":", "") for col in gdp.columns]

convert_hospital_ratings_to_int()
convert_gdp_to_float()

grouped = hospital.groupby(['Facility_City', 'Facility_State'], as_index=False).mean()

#ROUND TO INTS
for col, type_ in dict(grouped.dtypes).items():
    if str(type_)== 'float64':
       grouped [col] =grouped [col].apply(lambda x: normal_round(x))
       grouped [col] =grouped [col].fillna(-1)


grouped[['Rating_Overall','Rating_Timeliness','Rating_Mortality','Procedure_Pneumonia_Quality']] = grouped[['Rating_Overall','Rating_Timeliness','Rating_Mortality','Procedure_Pneumonia_Quality']].astype(str)

dummies = pd.get_dummies(grouped[['Rating_Overall','Rating_Timeliness','Rating_Mortality','Procedure_Pneumonia_Quality']], drop_first=True)
grouped = grouped[['Facility_State', 'Facility_City', 'Procedure_Pneumonia_Cost']]

new = grouped.merge(dummies, how='inner', left_on=grouped.index, right_on=dummies.index)

merged_df = new.merge(state, how='inner', left_on=['Facility_State', 'Facility_City'], right_on=['state_id', 'city'])




x1 = confirmed.melt(id_vars=confirmed.columns[1:10], value_vars=confirmed.columns[11:len(confirmed.columns)])

grp=x1[['Province_State','Admin2','variable','value']].groupby(['Province_State', 'Admin2','variable']).sum()
grp.reset_index(level=0, inplace=True)
grp['date']=grp.index.get_level_values('variable')

grp['city']=grp.index.get_level_values('Admin2')

grp.reset_index(drop=True, inplace=True)

#grp['month'] =pd.to_datetime(grp['date']).dt.month
grp['month'] = grp['date'].apply(lambda x: int(x.replace("-", "/").split("/")[0]))
grp['year'] = grp['date'].apply(lambda x: int(x.replace("-", "/").split("/")[2]))

grp['Quarter'] = pd.cut(grp['month'], bins=[0,4,7,10,12], labels=['Q1','Q2','Q3','Q4'])
grp['Quarter'] = grp['Quarter'].astype(str)+"_20"+grp['year'].astype(str)
    
'''
SPLIT TO 2020 ONLY HERE
''' 
grp = grp[grp['year']==20]
city_group = grp.groupby('city')


print("Looping over Cities present in both dataframes")
new_confirmed = pd.DataFrame()
for city, df in city_group:
    if city in list(merged_df.city):
        new_confirmed = new_confirmed.append(df)
print("Loop Completed")
final_confirmed = new_confirmed.groupby(['Province_State', 'city', 'Quarter'], as_index=False).sum()
final_confirmed=final_confirmed[["Province_State","city","Quarter","value"]]

final_confirmed.to_csv('final_confirmed.csv')

print("Completed")