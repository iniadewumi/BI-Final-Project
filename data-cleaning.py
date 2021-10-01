from ProjectData import GoogleSheets, DataUpdater
import pandas as pd
import requests
import io, os


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
           'Procedure_Heart_Failure_Value']].replace({'Higher':3, 'Average':2, 'Lower':1, 'Unknown':-1}).replace({'Better':3, 'Worse':1})

    print("Converting Hospital Ratings to Integers")    
    hospital[['Rating_Mortality', 'Rating_Safety',
           'Rating_Readmission', 'Rating_Experience', 'Rating_Effectiveness',
           'Rating_Timeliness', 'Rating_Imaging']] = hospital[['Rating_Mortality', 'Rating_Safety',
           'Rating_Readmission', 'Rating_Experience', 'Rating_Effectiveness',
           'Rating_Timeliness', 'Rating_Imaging']].replace({'Below':0, 'Same':1, 'None':-1, 'Above':2})
                
    # print(hospital.dtypes)

def convert_gdp_to_float():
    quarters = [x for x in gdp.columns if "Q" in x]
    gdp[quarters] = gdp[quarters].astype(str).replace('[\$,]', '', regex=True).astype(float)
    gdp.columns = [col.replace(":", "") for col in gdp.columns]

convert_hospital_ratings_to_int()
convert_gdp_to_float()


hospital = hospital[['Facility_Name', 'Procedure_Heart_Attack_Cost',
       'Procedure_Heart_Failure_Cost', 'Facility_State', 'Facility_City',
       'Rating_Overall', 'Procedure_Pneumonia_Quality',
       'Procedure_Pneumonia_Cost', 'Rating_Mortality',
        'Rating_Effectiveness', 'Rating_Timeliness', 'Procedure_Pneumonia_Value']]


pd.get_dummies(hospital)








test = hospital.groupby(['Facility_City', 'Facility_State'],as_index=False).mean()

state = state[['state_id', 'city', 'state_name', 'population', 'density']]


[x for x in hospital.columns if x not in test.columns]
#there were only 4000+ hospitals with ratings. So, we did an inner join to keep the data we needed only from the Massive hospital data file


