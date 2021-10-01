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

                
    # print(hospital.dtypes)

def convert_gdp_to_float():
    quarters = [x for x in gdp.columns if "Q" in x]
    gdp[quarters] = gdp[quarters].astype(str).replace('[\$,]', '', regex=True).astype(float)
    gdp.columns = [col.replace(":", "") for col in gdp.columns]


hospital['Rating_Overall'] = hospital['Rating_Overall'].astype(str)

h = pd.get_dummies(hospital[['Rating_Overall','Rating_Timeliness','Rating_Mortality','Procedure_Pneumonia_Quality']], drop_first=True)
hospital = hospital[['Facility_Name', 'Facility_State', 'Facility_City', 'Procedure_Pneumonia_Cost', 'Rating_Overall']]


new = h.merge(hospital, how='inner', left_on=h.index, right_on=hospital.index)

[x for x in hospital.columns if x not in test.columns]
#there were only 4000+ hospitals with ratings. So, we did an inner join to keep the data we needed only from the Massive hospital data file


