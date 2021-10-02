There were only 4000+ hospitals with ratings. So, we did an inner join to keep the data we needed only from the Massive hospital data file

We attempted to mergeon hospital names and ran into many issues such as duplicate hospital names across cities and states, 
1. Some hospitals were missing ratings
2. All hospitals are a time series, while hospital ratings are not.
3. Hospitals are named differently in both dataframes.


FRED
https://fred.stlouisfed.org/release/tables?eid=259515&rid=249#


TRIED CREATING DUMMIES. RAN INTO ULTIPLE ISSUES. RAN INTO MULTIPLE ISSUES
1. Aggregation into states. There are missing values taged -1, which would mess the dataframe up. We do not know the methodology for data collection and do not want to attempt to give ficticious ratings to hospitals. So, we decided to create dummies to account for the variables not being measured. This
Upon creating dummies. We intended to aggregate into cities as we could not do anything with direct hospital ratings. (we need to know how a city fairs as a whole). 

First issue, upon aggregation, we start finding values of 0.5, 0.66 in the dummy variables, so aggregation has to be done before creating the dummy variables but we run into the issues of having way too many columns and over-creating dummies. 



Dummies are not created when the valies are numeric, so we need to convert to strings, but this raises issues when we try to aggregate the values (if we do with strings, we run into issues). 


Eventually decided to replace all -1 with NaNs in original values and replace None and Unknowns with NaNs as well 

DATETIME WAS SLOW AF. SO WE DECIDED TO FIND A WAY AROUND IT. TRIED A COUPLE OF OPTIONS.
1. Pd.to_datetime. Took forever
2. Datetime.Datetime library wouldnt take our format as there were no leading '0s' We tried muitple methods, but Linux differs from windows.
3. Finally did string manipulation instead. This worked as needed. Ten times faster. 

