from ProjectData import GoogleSheets, DataUpdater
import pandas as pd
import numpy as np
import requests
import io, os
import math


try:
    from sklearn.linear_model import LogisticRegression
except:
    import pip
    
    package = "sklearn"
    if hasattr(pip, 'main'):
        pip.main(['install', package])
    else:
        pip._internal.main(['install', package])
        
    from sklearn.linear_model import LogisticRegression


class Regression:
    def __init__(self):
        pass

    def 
