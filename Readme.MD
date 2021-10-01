There were only 4000+ hospitals with ratings. So, we did an inner join to keep the data we needed only from the Massive hospital data file

We attempted to mergeon hospital names and ran into many issues such as duplicate hospital names across cities and states, 
1. Some hospitals were missing ratings
2. All hospitals are a time series, while hospital ratings are not.
3. Hospitals are named differently in both dataframes.


FRED
https://fred.stlouisfed.org/release/tables?eid=259515&rid=249#

