from ProjectData import GoogleSheets
import pandas as pd

data = GoogleSheets().dataframes()
df_names = [f"{str(k+1)}. {v}" for k,v in enumerate(data.keys())]

print("\nBelow are the available keys:")
print("\n".join(df_names))

death = data.get('covid_deaths_by_states_df')
hospital = data.get('hospital_ratings_df')

hospital.columns = [x.replace(".", "_").replace(" ", "_") for x in hospital.columns]

hospital[['Procedure_Heart_Attack_Quality',
       'Procedure_Heart_Attack_Value', 'Procedure_Heart_Failure_Quality',
       'Procedure_Pneumonia_Quality', 'Procedure_Hip_Knee_Quality',
       'Procedure_Hip_Knee_Value', 'Procedure_Pneumonia_Value',
       'Procedure_Heart_Failure_Value']] = hospital[[ 'Procedure_Heart_Attack_Quality',
       'Procedure_Heart_Attack_Value', 'Procedure_Heart_Failure_Quality',
       'Procedure_Pneumonia_Quality', 'Procedure_Hip_Knee_Quality',
       'Procedure_Hip_Knee_Value', 'Procedure_Pneumonia_Value',
       'Procedure_Heart_Failure_Value']].replace({'Higher':3, 'Average':2, 'Lower':1, 'Unknown':-1}).replace({'Better':3, 'Worse':1})

hospital[['Rating_Mortality', 'Rating_Safety',
       'Rating_Readmission', 'Rating_Experience', 'Rating_Effectiveness',
       'Rating_Timeliness', 'Rating_Imaging']] = hospital[['Rating_Mortality', 'Rating_Safety',
       'Rating_Readmission', 'Rating_Experience', 'Rating_Effectiveness',
       'Rating_Timeliness', 'Rating_Imaging']].replace({'Below':0, 'Same':1, 'None':-1, 'Above':2})
print(hospital.dtypes)


