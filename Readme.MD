INSTRUCTIONS

If git is not installed:

LINUX
* sudo yum install git OR sudo apt-get install git

WINDOWS
* Follow link to download: https://git-scm.com/download/win

INITIALIZE GIT
* git config --global user.name <Username>
* git config --global user.email <email>


CLONE GITHUB REPO
1. git clone https://github.com/iniadewumi/BI-Final-Project.git

CREATE VIRTUAL ENVIRONMENT 
1. pip install virtualenv OR pip3 install virtualenv
2. virtualenv RunBIFinal OR python3 -m virtualenv RunBIFinal
3. ./BITest/Scripts/activate or source ./RunBIFinal/bin/activate


INSTALL REQUIREMENTS
1. cd <path>/BI-Final-Project
2. pip install -r requirements.txt OR pip3 install -r requirements.txt
3. python3 main.py OR python main.py


BUILT WITH:

Python:
1. Data gotten with requests,io and Google Cloud API libraries like gspread, gspread_dataframe 
2. Python(pandas,sklearn,numpy,math)
3. Tableau

GETTING STARTED:

All the libraries are imported through code 

git clone https://github.com/iniadewumi/BI-Final-Project.git

Credentials for google spreadsheets are generated as needed

ABOUT THE PROJECT:

This project helps us to get great insights on covid and state demographivs data.Used web scrapping, data cleaning,data processing, visualization to acheive the goals of the project

ProjectData.py

Below are sources where the input data is taken from

FRED https://fred.stlouisfed.org/release/tables?eid=259515&rid=249#

STATE DATA
https://simplemaps.com/data/us-cities

https://raw.githubusercontent.com/iniadewumi/BI-Final-Project/master/uscities.csv

COVID DATA
https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_US.csv

https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_US.csv

https://docs.google.com/spreadsheets/d/1Ae4ufvsUpJfCpLvI3GGpLubwXadDQPVjj8sdW0zPRug/edit#gid=86086561

https://docs.google.com/spreadsheets/d/16osjIssbkqSeYMzMeKuH4erQFwxKGdpKgULS2HgQS_Q/edit#gid=0


Some of the datasets contains timeseries data,So, we are fetching the data from urls to get the most updated data
and the constant data has been saved in google sheets and code has been written to collect the data from google sheets using gspread.Using the same this package aslo contains the method to process the output data to google sheet.Cleaning the dataframe column names

methods includes the transformation of the data taken from various sources to dataframes here

data-cleaning.py

All the dataframes are collected by importing the classes from  ProjectData.py

Written normal_round method to round off the numerics ,as it has been used multiple times

Hospital data cleaning

All the hospitals Ratings dataset has the columns(quality and rating columns) with categorical data in them we have converted them to numerics.please find the conversions

'Higher':3, 'Average':2, 'Lower':1, 'Unknown':np.nan 'Better':3, 'Worse':1

'Below':0, 'Same':1, 'None':np.nan, 'Above':2

Rating_Mortality_0.0 => Below Average
Rating_Mortality_1.0 => Same as Average
Rating_Mortality_2.0 => Above Average

Rating_Timeliness_0.0 => Below Average
Rating_Timeliness_1.0 => Same as Average
Rating_Timeliness_2.0 => Above Average

Rating_Overall_1.0 => 1 Rating_Overall_2.0 => 2 Rating_Overall_3.0 => 3
Rating_Overall_4.0 => 4 Rating_Overall_5.0 => 5

Procedure_Pneumonia_Quality_1.0 => Better or Higher Procedure_Pneumonia_Quality_2.0 => Average Procedure_Pneumonia_Quality_3.0 => Worse or Lower

Dummies are not created when the values are numeric, so we need to convert to strings, but this raises issues when we try to aggregate the values (if we do with strings, we run into issues). 

Facility_State
Facility_Type
Facility_City
Procedure_Pneumonia_Quality
Rating_Mortality
Rating_Safety
Rating_Readmission
Rating_Experience
Rating_Effectiveness
Rating_Timeliness

Eventually decided to replace all -1 with NaNs in original values and replace None and Unknowns with NaNs as well and dummies are created on these columns

To find the missing values of overall_Ratings of hospitals ,we have dropped all the null values to from the above columns and merged with overall rating to predict the missing values(train_df) .For this , we have sent this dataframe(train_df) to regression.py class where methods were written for logistic and linear regression models.

We tested the predcited data with both models , could notice that logistic has more outliers than linear but the linear regression model is giving the values more than 5.Both the models have been used based on the result value

TRIED CREATING DUMMIES. RAN INTO MULTIPLE ISSUES. RAN INTO MULTIPLE ISSUES

Aggregation into states. There are missing values taged -1, which would mess the dataframe up. We do not know the methodology for data collection and do not want to attempt to give ficticious ratings to hospitals. So, we decided to create dummies to account for the variables not being measured. This Upon creating dummies. We intended to aggregate into cities as we could not do anything with direct hospital ratings. (we need to know how a city fairs as a whole).
First issue, upon aggregation, we start finding values of 0.5, 0.66 in the dummy variables, so aggregation has to be done before creating the dummy variables but we run into the issues of having way too many columns and over-creating dummies.

There were only 4000+ hospitals with ratings. So, we did an inner join to keep the data we needed only from the Massive hospital data file

We attempted to merge on hospital names and ran into many issues such as duplicate hospital names across cities and states,

Some hospitals were missing ratings
All hospitals are a time series, while hospital ratings are not.
Hospitals are named differently in both dataframes.


Covid datasets transformation
Method has been written to covert the daily data (columns) to row wise quarterly data,used melt and aggregated count with state ,city and quarter 


DATETIME WAS SLOW AF. SO WE DECIDED TO FIND A WAY AROUND IT. TRIED A COUPLE OF OPTIONS.

Pd.to_datetime. Took forever
Datetime.Datetime library wouldnt take our format as there were no leading '0s' We tried muitple methods, but Linux differs from windows.
Finally did string manipulation instead. This worked as needed. Ten times faster.

GDP data cleaning and transformation

The amount in GDP data has been cleaned from '$' and converted to float from string

the quater data was present in column format , so written the code to have identify the quarter columns (renamed them by replacing to adjust with other dataset quarter columns) and then melted ,aggregated for 2020 data .The avg_income and GDP data for each state quarterly has been merged with covid and hospital data

The outputs each datasets has been saved to sheets in google spreedsheet and final_output (all datasets inner joined) slao has been sent to the sheet method


All the visualization and analysis on the outputs have been performed using Tableau 

