from ProjectData import GoogleSheets, DataUpdater
import pandas as pd
import numpy as np
import requests
import io, os
import math


#calling class and methods in ProjectData to load data from sources to dataframes

sheets = GoogleSheets()
data = sheets.dataframes()
df_names = [f'{k + 1}. {v}' for k,v in enumerate(data.keys())]

print("\nBelow are the available keys to get each dataframe:")
print("\n".join(df_names))


gdp = data.get('gdp_sheet_df')
hospital = data.get('hospital_ratings_df')
deaths = data.get('deaths')
confirmed = data.get('confirmed')
state = data.get('state')


def normal_round(n):
    """rounds to avoid decimals

    Args:
        n ([float])

    Returns:
        rounded value
    """    
    try:
        return math.floor(n) if n - math.floor(n) < 0.5 else math.ceil(n)
    except ValueError:
        return np.nan

def convert_hospital_ratings_to_int():
    """Transforming the categorical data to numerical data
    """        
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
    hospital['Rating_Overall'] = hospital['Rating_Overall'].replace('', np.nan)
    hospital['Rating_Overall'] = hospital['Rating_Overall'].astype(float)
    hospital['Rating_Overall'] = hospital['Rating_Overall'].replace(-1, np.nan)

    # print(hospital.dtypes)

def CleaningGDPData():
    """The amount in GDP data has been cleaned from '$' and converted to float from string and renamed the column names
    """    
    quarters = [x for x in gdp.columns if "Q" in x]
    gdp[quarters] = gdp[quarters].astype(str).replace('[\$,]', '', regex=True).astype(float)
    gdp.columns = [col.replace(":", "_") for col in gdp.columns]

convert_hospital_ratings_to_int()
CleaningGDPData()

print("\n\nGetting Dummies and Running Regresion, This might take a while...")

rating_overall = hospital['Rating_Overall']

dummy_list = hospital[['Facility_State', 'Facility_Type', 'Facility_City', 'Procedure_Pneumonia_Quality', 'Rating_Mortality', 'Rating_Safety',
                       'Rating_Readmission', 'Rating_Experience', 'Rating_Effectiveness', 'Rating_Timeliness']].fillna(-1)
dummy_x = pd.get_dummies(dummy_list.astype(str), drop_first=True)

X = dummy_x.reset_index()

#NOTE DROPPING EMPTY Y VALUES
train_df = rating_overall.dropna().reset_index().merge(X).drop(['index'], axis=1)

predict_df = rating_overall[rating_overall.isna()].reset_index().merge(X)

from regression import Regression

reg = Regression(train_df)
reg.Log()
reg.Lin()

print('Training Complete')

# lin = reg.lin_predict(train_df)
# log = reg.log_predict(train_df)
        
# out = pd.DataFrame({'Pred_Log':log,'Pred_Lin':lin, 'Act': train_df['Rating_Overall']})
# out['Pred_Lin'] = out['Pred_Lin'].apply(lambda x: normal_round(x))

# out['diff_lin'] = abs(out['Act'] - out['Pred_Lin'])
# out['diff_log'] = abs(out['Act'] - out['Pred_Log'])

# print(f"Summäry for Linear\n{out['diff_lin'].describe()}")
# print(f"Summäry for Log\n{out['diff_log'].describe()}")

t = predict_df.head()
cols = t.loc[:, ~t.columns.isin(['Rating_Overall', 'index'])].columns

print('Predicting Missing Values...')
for index, row in predict_df.iterrows():
    inp = row[cols]
    pred = reg.lin_predict([inp])[0]
    if abs(pred)>5:
        pred = reg.log_predict([inp])[0]
    hospital.at[row['index'], 'Rating_Overall'] = normal_round(pred)
    
    
gdp.drop(['GeoFips', 'LineCode','Description'], axis=1)
grouped = hospital.groupby(['Facility_City', 'Facility_State'], as_index=False).mean()

#ROUND TO INTS
for col, type_ in dict(grouped.dtypes).items():
    if str(type_)== 'float64':
       grouped [col] =grouped [col].apply(lambda x: normal_round(x))
       grouped [col] =grouped [col].fillna(-1)
       


grouped[['Rating_Overall','Rating_Timeliness','Rating_Mortality','Procedure_Pneumonia_Quality']] = grouped[['Rating_Overall','Rating_Timeliness','Rating_Mortality','Procedure_Pneumonia_Quality']].astype(str)
dummies = pd.get_dummies(grouped[['Rating_Overall','Rating_Timeliness','Rating_Mortality','Procedure_Pneumonia_Quality']], drop_first=True)
grouped = grouped[['Facility_State', 'Facility_City', 'Procedure_Pneumonia_Cost', 'Rating_Overall']]


grouped = grouped.reset_index().rename(columns={'index':'mergekeys'})
dummies = dummies.reset_index().rename(columns={'index':'mergekeys'})
new = grouped.merge(dummies, how='inner', left_on=grouped['mergekeys'], right_on=dummies['mergekeys'])
final_hosp_state = grouped.merge(state, how='inner', left_on=['Facility_State', 'Facility_City'], right_on=['state_id', 'city'])



try:
    final_hosp_state = final_hosp_state.drop(['Facility_State', 'key_0', 'mergekeys_x', 'mergekeys_y', 'city_ascii', 'county_fips', 'source', 'incorporated', 'timezone', 'ranking', 'id',  'lat', 'lng', 'military', 'zips'], axis=1)
except:
    final_hosp_state = final_hosp_state.drop(['mergekeys', 'Facility_State', 'city_ascii', 'county_fips', 'source', 'incorporated', 'timezone', 'ranking', 'id',  'lat', 'lng', 'military', 'zips'], axis=1)
    
print("\nProcessing Confirmed Cases...")


def COVID_Dataset_Trans(old_df, start):
    """ melts and aggregates into quarterly data

    Args:
        old_df ([dataframe]): input dataframes
        start ([int]): column number , from where it need to be melted

    Raises:
        Exception: Dataframe check

    Returns:
        Dataframe
    """    
    if str(type(old_df))!="<class 'pandas.core.frame.DataFrame'>":
        print("\n\n\n\nF")
        raise Exception("Not a valid Dataframe")
        
    melted = old_df.melt(id_vars=old_df.columns[1:start-1], value_vars=old_df.columns[start:len(old_df.columns)])

    '''
    NOT USING TO_DATETIME. IT WAS SLOW ON THE LARGER DATAFRAME. USING SPLIT AS DATE DATA IS CONSISTENT
    x1['Quarter'] = pd.cut(pd.to_datetime(x1['variable']).dt.month, bins=[0,4,7,10,12], labels=['Q1','Q2','Q3','Q4'])
    x1['Quarter'] = x1['Quarter'].astype(str)+'_'+pd.to_datetime(x1['variable']).dt.year.astype(str)
    '''

    melted['month'] = melted['variable'].apply(lambda x: int(x.replace("-", "/").split("/")[0]))
    melted['year'] = melted['variable'].apply(lambda x: int(x.replace("-", "/").split("/")[2]))

    melted['Quarter'] = pd.cut(melted['month'], bins=[0,4,7,10,12], labels=['Q1','Q2','Q3','Q4'])
    melted['Quarter'] = "20"+melted['year'].astype(str)+'_'+melted['Quarter'].astype(str)
    melted = melted[melted['year']==20]


    grp=melted[['Province_State','Admin2' ,'Quarter', 'value']].groupby(['Province_State','Admin2','Quarter']).max()
    grp['city']=grp.index.get_level_values('Admin2')
    grp['Province_State']=grp.index.get_level_values('Province_State')
    grp['Quarter']=grp.index.get_level_values('Quarter')
    grp.reset_index(drop=True, inplace=True)

    city_group = grp.groupby(['city','Province_State'])
    print("Looping over cities present in New hospital-state and covid dataframe")
    new_df = pd.DataFrame()
    city_state = tuple(zip(list(final_hosp_state['city']), list(final_hosp_state['state_name'])))

    for city, df in city_group:
        #print(df)
        if city in city_state:
            new_df = new_df.append(df)
    print("Loop Completed")

    return new_df


new_confirmed = COVID_Dataset_Trans(confirmed, start=11)
final_confirmed = new_confirmed.groupby(['Province_State', 'city', 'Quarter'], as_index=False).sum()
final_confirmed=final_confirmed[["Province_State","city","Quarter","value"]]
final_confirmed = final_confirmed.rename(columns={'value':'Confirmed_Cases'})

print("\nProcessing Deaths...")
new_deaths = COVID_Dataset_Trans(deaths, start=12)
final_deaths = new_deaths.groupby(['Province_State', 'city', 'Quarter'], as_index=False).sum()
final_deaths = final_deaths[["Province_State","city","Quarter","value"]]
final_deaths = final_deaths.rename(columns={'value':'Deaths'})


final_covid = final_confirmed.merge(final_deaths)
final_covid['Recovered(conf - deaths)'] = final_covid['Confirmed_Cases']-final_covid['Deaths']


gdp2=pd.melt(gdp, id_vars=['GeoName','Avg_Income_(2020)'], value_vars=['2019_Q1', '2019_Q2','2019_Q3', '2019_Q4', '2020_Q1', '2020_Q2', '2020_Q3', '2020_Q4', '2021_Q1'], var_name='Quarter', value_name='GDP_Data', col_level=None)

def get_2020(x):
    return '2020' in str(x)

#GET 2020 Data Only
gdp2 = gdp2[gdp2['Quarter'].apply(lambda x: get_2020(x))]


hosp_conf = final_hosp_state.merge(final_covid, left_on=['city', 'state_name'], right_on=['city', 'Province_State'])

final_output=hosp_conf.merge(gdp2, left_on=['state_name', 'Quarter'], right_on=['GeoName', 'Quarter'], how='left')
final_output = final_output.sort_values('GDP_Data')

final_output.drop(['Facility_City', 'GeoName', 'Province_State'], axis=1, inplace=True)

gdp2 = gdp2[['Quarter', 'GeoName', 'GDP_Data', 'Avg_Income_(2020)']]
final_confirmed = final_confirmed[['Quarter', 'Province_State', 'city', 'Confirmed_Cases']]
final_deaths = final_deaths[['Quarter','Province_State', 'city', 'Deaths']]
final_covid = final_covid[['Quarter', 'Province_State', 'city', 'Confirmed_Cases', 'Deaths', 'Recovered(conf - deaths)']]
final_hosp_state = final_hosp_state[['state_name', 'state_id','state_name', 'county_name', 'population', 'density']]
final_output = final_output[['Quarter', 'Procedure_Pneumonia_Cost', 'Rating_Overall', 'city', 'state_id','state_name', 'county_name', 'population',	'density','Confirmed_Cases', 'Deaths','Recovered(conf - deaths)', 'Avg_Income_(2020)', 'GDP_Data']]

final_output['Adjusted Death'] = final_output['Deaths']/final_output['population']
final_output['Adjusted Confirmed'] = final_output['Confirmed_Cases']/final_output['population']

sheets.create_output('GDP', df=gdp2)
sheets.create_output('Confirmed', df=final_confirmed)
sheets.create_output('Deaths', df=final_deaths)
sheets.create_output('Covid Combined', df=final_covid)

sheets.create_output('Hospital-State', df=final_hosp_state)
sheets.create_output('FULL-DATASET', df=final_output)


import webbrowser
webbrowser.open(sheets.output_spreadsheet.url)
print("Completed")