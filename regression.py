from ProjectData import GoogleSheets, DataUpdater
import pandas as pd
import numpy as np
import requests
import io, os
import math

from sklearn.linear_model import LogisticRegression, LinearRegression



class Regression:
    def __init__(self, df):
        try:
            import sklearn
        except Exception as e:
            import pip
            
            package = "sklearn"
            if hasattr(pip, 'main'):
                pip.main(['install', package])
            else:
                pip._internal.main(['install', package])
                
            from sklearn.linear_model import LogisticRegression, LinearRegression
        self.X = df.loc[:, ~df.columns.isin(['Rating_Overall', 'index'])]
        self.y = df['Rating_Overall']

    def Log(self):
        self.log_model = LogisticRegression(max_iter=500).fit(self.X, self.y)
        print(f'\nLog Score: {self.log_model.score(self.X, self.y)}')

    def Lin(self):
        self.lin_model = LinearRegression().fit(self.X, self.y)
        print(f'\nLin Score: {self.lin_model.score(self.X, self.y)}')

    def lin_predict(self, df):
        if type(df) == list:
            X = df
            return self.lin_model.predict(X)
        X = df.loc[:, df.columns!='Rating_Overall']
        return self.lin_model.predict(X)


    def log_predict(self, df):
        if type(df) == list:
            X = df
            return self.log_model.predict(X)
        X = df.loc[:, df.columns!='Rating_Overall']
        return self.log_model.predict(X)