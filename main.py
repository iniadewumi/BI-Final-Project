from ProjectData import GoogleSheets, DataUpdater
import pandas as pd
import numpy as np
import requests
import io, os
import math




class ProjectWorkFlow:

    def __init__(self):

        #calling class and methods in ProjectData to load data from sources to dataframes
        """
        [summary]

        All Extraction is performed in __init__() as the needed data for cleaning, transform and load need to be collected here... 
        Should this fail, no point in having the rest of the class methods or objects.
        """
        self.sheets = GoogleSheets()
        self.data = self.sheets.dataframes()
        self.df_names = [f'{k + 1}. {v}' for k,v in enumerate(self.data.keys())]

        print("\nBelow are the available keys to get each dataframe:")
        print('\n'.join(self.df_names))


        self.gdp = self.data.get('gdp_sheet_df')
        self.hospital = self.data.get('hospital_ratings_df')
        self.deaths = self.data.get('deaths')
        self.confirmed = self.data.get('confirmed')
        self.state = self.data.get('state')


    def convert_hospital_ratings_to_int(self):
        """
        Transforming the categorical data to numerical data
        """        
        print("\nConverting Hospital Procedure Quality to Integers")
        self.hospital[['Procedure_Heart_Attack_Quality',
            'Procedure_Heart_Attack_Value', 'Procedure_Heart_Failure_Quality',
            'Procedure_Pneumonia_Quality', 'Procedure_Hip_Knee_Quality',
            'Procedure_Hip_Knee_Value', 'Procedure_Pneumonia_Value',
            'Procedure_Heart_Failure_Value']] = self.hospital[[ 'Procedure_Heart_Attack_Quality',
            'Procedure_Heart_Attack_Value', 'Procedure_Heart_Failure_Quality',
            'Procedure_Pneumonia_Quality', 'Procedure_Hip_Knee_Quality',
            'Procedure_Hip_Knee_Value', 'Procedure_Pneumonia_Value',
            'Procedure_Heart_Failure_Value']].replace({'Higher':3, 'Average':2, 'Lower':1, 'Unknown':np.nan}).replace({'Better':3, 'Worse':1})

        print("\nConverting Hospital Ratings to Integers")    
        self.hospital[['Rating_Mortality', 'Rating_Safety',
            'Rating_Readmission', 'Rating_Experience', 'Rating_Effectiveness',
            'Rating_Timeliness', 'Rating_Imaging']] = self.hospital[['Rating_Mortality', 'Rating_Safety',
            'Rating_Readmission', 'Rating_Experience', 'Rating_Effectiveness',
            'Rating_Timeliness', 'Rating_Imaging']].replace({'Below':0, 'Same':1, 'None':np.nan, 'Above':2})
        self.hospital['Rating_Overall'] = self.hospital['Rating_Overall'].replace('', np.nan)
        self.hospital['Rating_Overall'] = self.hospital['Rating_Overall'].astype(float)
        self.hospital['Rating_Overall'] = self.hospital['Rating_Overall'].replace(-1, np.nan)

        # print(hospital.dtypes)

    def CleaningGDPData(self):
        """
        The amount in GDP data has been cleaned from '$' and "," and converted to float from string and renamed the column names
        """    
        quarters = [x for x in self.gdp.columns if "Q" in x]
        self.gdp[quarters] = self.gdp[quarters].astype(str).replace('[\$,]', '', regex=True).astype(float)
        self.gdp.columns = [col.replace(":", "_") for col in self.gdp.columns]



    def RegressionFill(self):
        """
        [summary]     
        -------

        This breaks down the hospital rating dataframe into columns we found to be important in prediction.
        These columns were selected by running individual regresion on each column and viewing its correlation and R-Square
        After this was done, various combinations of columns were attempted until we got the current selection.

        After training, we found the Linear Regression to be more effective, but when the Linera Model fails, it throws out wide outliers
        To combat this, we used Linear as a default to predict missing values, but when the output is an outlier, 
        we default to the Logistic Regression Model to find that prediction for that missing value.

        Returns
        -------
        None.

        Prints
        -------
        The Regression score for both Linear and Logistic Regressions. 

        """

        print("\n\nGetting Dummies and Running Regresion, This might take a while...")

        rating_overall = self.hospital['Rating_Overall']
        dummy_list = self.hospital[['Facility_State', 'Facility_Type', 'Facility_City', 'Procedure_Pneumonia_Quality', 'Rating_Mortality', 'Rating_Safety',
                            'Rating_Readmission', 'Rating_Experience', 'Rating_Effectiveness', 'Rating_Timeliness']].fillna(-1)
        dummy_x = pd.get_dummies(dummy_list.astype(str), drop_first=True)
        X = dummy_x.reset_index()

        #NOTE DROPPING EMPTY Y VALUES
        train_df = rating_overall.dropna().reset_index().merge(X).drop(['index'], axis=1)
        predict_df = rating_overall[rating_overall.isna()].reset_index().merge(X)

        from regression import Regression

        reg = Regression(train_df)
        print('\nTraining...')
        reg.Log()
        reg.Lin()
        print("Training Complete!")
        all_cols = predict_df.head()
        cols_for_input = all_cols.loc[:, ~all_cols.columns.isin(['Rating_Overall', 'index'])].columns

        print('\nPredicting Missing Values...')
        for _, row in predict_df.iterrows():
            inp = row[cols_for_input]
            pred = reg.lin_predict([inp])[0]
            if abs(pred)>5:
                pred = reg.log_predict([inp])[0]
            self.hospital.at[row['index'], 'Rating_Overall'] = normal_round(pred)
            
        print("Missing Values have been filled!")


    def AggregateHospitalState(self):
        """
        [summary]
        --------------
        This uses both the Hospital Ratings DataFrame and the State DataFrame (containing city population, density, etc.)
        First, we aggregate the full hospital ratings data for each City in each State (i.e gets the average rating of hospitals in each city)
        
        Next, we round each columns that contains floats into integers using our normal_round function
        Next, we create dummy variables for the columns we think are most important (For people that may need those later, maybe for a better prediction algorithm).
        Next, we merge both the state and hospital dataframes together. 



        Returns
        --------------
        None.

        """
        grouped = self.hospital.groupby(['Facility_City', 'Facility_State'], as_index=False).mean()

        #ROUND ALL COLUMN VALUES TO INTS
        for col, type_ in dict(grouped.dtypes).items():
            if str(type_)== 'float64':
                grouped [col] =grouped [col].apply(lambda x: normal_round(x))
                grouped [col] =grouped [col].fillna(-1)
                


        grouped[['Rating_Overall','Rating_Timeliness','Rating_Mortality','Procedure_Pneumonia_Quality']] = grouped[['Rating_Overall','Rating_Timeliness','Rating_Mortality','Procedure_Pneumonia_Quality']].astype(str)
        dummies = pd.get_dummies(grouped[['Rating_Overall','Rating_Timeliness','Rating_Mortality','Procedure_Pneumonia_Quality']], drop_first=True)
        grouped = grouped[['Facility_State', 'Facility_City', 'Procedure_Pneumonia_Cost', 'Rating_Overall']]


        grouped = grouped.reset_index().rename(columns={'index':'mergekeys'})
        dummies = dummies.reset_index().rename(columns={'index':'mergekeys'})
        self.hospital_with_dummies = grouped.merge(dummies, how='inner', left_on=grouped['mergekeys'], right_on=dummies['mergekeys'])
        self.final_hosp_state = grouped.merge(self.state, how='inner', left_on=['Facility_State', 'Facility_City'], right_on=['state_id', 'city'])


        #NOTE, WE WANTED TO MAKE SURE ALL NON-RELEVANT COLUMNS ARE DROPPED, HENCE, THE TRY & EXCEPT
        try:
            self.final_hosp_state = self.final_hosp_state.drop(['Facility_State', 'key_0', 'mergekeys_x', 'mergekeys_y', 'city_ascii', 'county_fips', 'source', 'incorporated', 'timezone', 'ranking', 'id',  'lat', 'lng', 'military', 'zips'], axis=1)
        except:
            self.final_hosp_state = self.final_hosp_state.drop(['mergekeys', 'Facility_State', 'city_ascii', 'county_fips', 'source', 'incorporated', 'timezone', 'ranking', 'id',  'lat', 'lng', 'military', 'zips'], axis=1)
            


    def COVID_Dataset_Trans(self, old_df, start):
        """ 
        Melts and aggregates, then determines the quarter that each date falls into in the year.
        Next, we get the total of the records for each quarter in each city in each state.
        Next, we filter for ONLY cities that are in the self.final_hosp_state dataframe, as there is no point in keeping cities without rated hospitals and population, density data
        
        NOTE: We keep only the maximum records of cases in each of the Quarters in the year. 
        NOTE: This function applies to both death and confirmed cases dataframes, as they have similar formats
        NOTE: Only cities with rated hospitals are processed!

        Args:
        ---------------
        old_df ([dataframe]): input dataframes
        start ([int]): column number , from where it need to be melted

        Raises:
        ---------------
            Exception: Dataframe check

        Returns:
        ---------------
            new_df : Melted Dataframe with ["State", "City", "Quarter", "Cases or Deaths"]
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
        print("\nLooping over cities present in New hospital-state and covid dataframe")
        new_df = pd.DataFrame()
        city_state = tuple(zip(list(self.final_hosp_state['city']), list(self.final_hosp_state['state_name'])))

        for city, df in city_group:
            #print(df)
            if city in city_state:
                new_df = new_df.append(df)
        print("\nLoop Completed")

        new_df = new_df.groupby(['Province_State', 'city', 'Quarter'], as_index=False).sum()
        return new_df[["Province_State", "city", "Quarter", "value"]]


    def get_clean_covid_dfs(self):
        # WE RUN THE FUNCTION AGAINST BOTH DATAFRAMES, THEN, GENERATE THE PSEUDO RECOVERED (JUST A SUBTRACTION) NOT ACTUAL RECOVERIES!!!
        print("\nProcessing Confirmed Cases...")
        new_confirmed = self.COVID_Dataset_Trans(old_df=self.confirmed, start=11)
        self.final_confirmed = new_confirmed.rename(columns={'value':'Confirmed_Cases'})

        print("\nProcessing Deaths...")
        new_deaths = self.COVID_Dataset_Trans(old_df=self.deaths, start=12)
        self.final_deaths = new_deaths.rename(columns={'value':'Deaths'})

        print("\n\nGenerating Merged Death and Confirmed Cases DatFrame...\n")
        self.final_covid = self.final_confirmed.merge(self.final_deaths)
        self.final_covid['Diff (conf - deaths)'] = self.final_covid['Confirmed_Cases']-self.final_covid['Deaths']
    def melt_gdp(self):
        """
        [summary]
        -------
        Melts GDP DataSet and ensures that we only keep 2020 Quarters only

        """
        self.gdp = self.gdp.drop(['GeoFips', 'LineCode','Description'], axis=1)
        self.cleaned_gdp_df=pd.melt(self.gdp, id_vars=['GeoName','Avg_Income_(2020)'], value_vars=['2019_Q1', '2019_Q2','2019_Q3', '2019_Q4', '2020_Q1', '2020_Q2', '2020_Q3', '2020_Q4', '2021_Q1'], var_name='Quarter', value_name='GDP_Data', col_level=None)
        self.cleaned_gdp_df = self.cleaned_gdp_df[self.cleaned_gdp_df['Quarter'].apply(lambda x: get_2020(x))]

    def create_full_data_merge(self):
        """
        [summary]
        -----------
        This merges all the dataframes into one final dataframe: 
        Containing: City Data, Hospital Ratings by City, Covid Cases and Deaths, GDP for each state, Avg Income in each state, etc.)
        All of this for each quarter in each Year 2020.


        NEXT:
        It sorts all the columns as specified for clean look
        """
        hosp_conf = self.final_hosp_state.merge(self.final_covid, left_on=['city', 'state_name'], right_on=['city', 'Province_State'])
        final_output = hosp_conf.merge(self.cleaned_gdp_df, left_on=['state_name', 'Quarter'], right_on=['GeoName', 'Quarter'], how='left')
        self.final_output = final_output.sort_values('GDP_Data')
        self.final_output.drop(['Facility_City', 'GeoName', 'Province_State'], axis=1, inplace=True)

        self.cleaned_gdp_df = self.cleaned_gdp_df[['Quarter', 'GeoName', 'GDP_Data', 'Avg_Income_(2020)']]
        self.final_confirmed = self.final_confirmed[['Quarter', 'Province_State', 'city', 'Confirmed_Cases']]
        self.final_deaths = self.final_deaths[['Quarter','Province_State', 'city', 'Deaths']]
        self.final_covid = self.final_covid[['Quarter', 'Province_State', 'city', 'Confirmed_Cases', 'Deaths', 'Diff (conf - deaths)']]
        self.final_hosp_state = self.final_hosp_state[['state_id','state_name', 'county_name', 'population', 'density', 'Rating_Overall']]
        self.final_output = self.final_output[['Quarter', 'state_id','state_name', 'county_name', 'city', 'GDP_Data', 'Avg_Income_(2020)', 'population', 'density', 'Rating_Overall','Confirmed_Cases', 'Deaths','Diff (conf - deaths)', 'Procedure_Pneumonia_Cost']]

        self.final_output['Adjusted Death'] = self.final_output['Deaths']/self.final_output['population']
        self.final_output['Adjusted Confirmed'] = self.final_output['Confirmed_Cases']/self.final_output['population']
    
    def create_updated_outputs(self):
        """
        [summary]
        ---------
        Generates output files to GoogleSheets for each dataframe
        """
        self.sheets.create_output('GDP', df=self.cleaned_gdp_df)
        self.sheets.create_output('Confirmed', df=self.final_confirmed)
        self.sheets.create_output('Deaths', df=self.final_deaths)
        self.sheets.create_output('Covid Combined', df=self.final_covid)

        self.sheets.create_output('Hospital-State', df=self.final_hosp_state)
        self.sheets.create_output('FULL-DATASET', df=self.final_output)

        try:
            import webbrowser
            webbrowser.open(self.sheets.output_spreadsheet.url)
        except:
            pass

        print("\n\nAll output dataframes can be found here:", self.sheets.output_spreadsheet.url)

    def run_cleans(self):
        self.convert_hospital_ratings_to_int()
        self.CleaningGDPData()
        self.melt_gdp()
        
    def run_transforms(self):
        self.RegressionFill()
        self.AggregateHospitalState()
        self.get_clean_covid_dfs()

    def run_merge_out(self):
        self.create_full_data_merge()
        self.create_updated_outputs()



#####################################
##     Other Functions below       ##
#####################################
def normal_round(n):
    """
    [summary]
    -------
    Rounds from 0.5.

    Args:
        n ([float])

    Returns:
        rounded value if valid, else, NaN value
    """    
    try:
        return math.floor(n) if n - math.floor(n) < 0.5 else math.ceil(n)
    except ValueError:
        return np.nan


def get_2020(x:str):
    """[summary]
    -------
    Returns True or False for year

    Args:
        x (str): [quarter string]

    Returns:
        [BOOL]: [True or False if Quarter is from Year 2020]
    """
    return '2020' in str(x)

def main():
    workflow = ProjectWorkFlow()
    workflow.run_cleans()
    workflow.run_transforms()
    workflow.run_merge_out()


if __name__ == '__main__':
    main()