import gspread, json
from gspread_dataframe import set_with_dataframe, get_as_dataframe
import pandas as pd

# pd.set_option("display.max_columns", None)




class GoogleSheets:
    def __init__(self):
        self.worksheets_gotten = False
        
        try:
            self.gc =gspread.service_account('ini_credentials.json')
        except:
            #WRITE CRED FILE
            cred = {
            "type": "service_account",
            "project_id": "total-now-302919",
            "private_key_id": "e7621a32b6508c634ed6fa2b937edd82f5e3636f",
            "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQC3b/P+UB+JHz0l\nL35eBgzymDgudLPhGGUemWO3pjOgfs3VRsO6+2tvBFds18sPeOkNk8rtAYT7/5lf\nfIFRGLOtD7mXw/+4/t8aGoThOGEcjC8WGpj1d93QxtBYSe8AeTkevwVGBcZMHnkR\nB13yrowweoNfae6Xj85OdlQFHZp8hsRrBr+pMWvVOmGOjLPE0f83C7lnVPIu7UdV\nBXHaNzH5oW6GRDKRmobOvCP/uHEagQQOh9ds9nvT4BOb23jR7nRFxizbrWIXm4oC\n5f5cFHyZmicL7xNov2Aez51H6Z7nD+BROAi/MtNApSgDAA1vxQurgUW+wlFXc/vK\n/TJHPNkvAgMBAAECggEAHY1cNh7guGhqIzj/tz8V9pP+YmxBDyUCeXJbGxrxW0xt\nIGHU+6UwnYM0tcodd9cnWJ10RkgwjUvpgMrKp0QprNpehdbji6wH86kiZN7+iikC\nwaFe/hYltqeo/hL0fajfQmxMzwdJdzmQ5eTqxkOc6Icm7ggKElcZH2aMdQHcFxkg\nfc/wKskosEpBytDieL5mkuSol/FlpDnhahEYVXVesHpMr3AWxPQNEE+ZY0+eunFP\n877F8EnDEMEjpE9tkn+Bxv08UEMBoYnJJoTWpt/PkGB3YWxBpdDMXeIlLiIa2mpP\nVZf+FX5W4+eLQOhzCKUyFYwjOhVpZqn6xkEF0BgYCQKBgQDk4Q6p8yhRCXKM/74O\n9BgKEQD8u5Rv/ePyUK88+jsropgF0UjhoUAie5RdYWQYtLA5QkIRJ84UrMgw2+1I\nI9vPfV9LHCb08bRUAmoPQJ8dOGFSQ18EBsvxlRovt79HMbKvrof1dw6ONK8BfH2P\nDc2+b7KgKQForAlyhn50M+jjKQKBgQDNLHB8ax4gizBZ/Dxt/I4nBE0Yp/hA6Z8a\nrDhfm7u8OtSBydPAtO3PnplPV+epsO04SUhhblLg5hEjvXGGX5e286QywZcmq4tn\nkh8trLFT8m0f+6e4E6r8i8oiEIMX/OEqm8cUwMt3FwP6t4sr3XPeBDl5kWPMUcRi\nM1hzjf98lwKBgQCunWEBmaglgw6osaf/YoxAic52AmnxswJH6PR4kfO4i5htv8hZ\nkxsJ87wyLc4e5yMW3AzpZ7PapCMq227AvdLCDsU32WeDMi/AdAMUVdnOgigFia6g\ntMq9KWLMCuRcXXcUfxPs2oL5TQpDGQX1sLNJ2Y+ujvorMC0Y+bDZ2IyRWQKBgQCg\nqkQLqfYVlelvDc5kcnj+pKeavy3v5wHoaSRb+h+w7oCqgdmH9iajhpaXQ0bt4tZg\nVUKQyUutQXv6eMcFaqXrZi8Wb/JlHcA0goBXy/uwuQ4rFW3o/73Ntcm7kyKVDjlk\nnEfxQGgUAbnkAwbAetHY8YXwZKG5xe362CpTksaWUQKBgCY5NEZWAhmhLFChp+DC\ntcdYDh5z84ZifTQCUbCAJvf0n9O+LtSJCJao8Mh0fTLfe7v83Kn8ZOFgqytScpr9\nBQE6dCt0MtWhnv3VwsS/vFkOZBuztHYdBXmL8NV/jAQuiB6dytjMNhj2ODJppPKh\nkAhSV8pkzIBjYjGMYMkx3vyM\n-----END PRIVATE KEY-----\n",
            "client_email": "1060310697081-compute@developer.gserviceaccount.com",
            "client_id": "101941567001344562717",
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
            "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
            "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/1060310697081-compute%40developer.gserviceaccount.com"
        }

            with open("ini_credentials.json", "w") as f:
                json.dump(cred, f)
            self.gc =gspread.service_account('ini_credentials.json')
    
    def get_worksheets(self, spreadsheet_url=None, worksheet_name=None):
        if not spreadsheet_url:
            self.spreadsheet = spreadsheet = self.gc.open_by_url('https://docs.google.com/spreadsheets/d/1Ae4ufvsUpJfCpLvI3GGpLubwXadDQPVjj8sdW0zPRug/edit#gid=86086561')
        else:
            spreadsheet = self.gc.open_by_url(spreadsheet_url)

        if worksheet_name:
            return spreadsheet.worksheet(worksheet_name)

        self.gdp_sheet = self.spreadsheet.worksheet('GDP Real Dollars')
        self.covid_deaths_by_states = self.spreadsheet.worksheet("Covid Deaths by State (1)")
        self.hospital_ratings = self.spreadsheet.worksheet("Hospital Ratings (1)")
        self.covid_infections_by_states = self.spreadsheet.worksheet("Covid Infections by State (1)")
        self.covid_recovery_by_state = self.spreadsheet.worksheet("Covid Recovery by State")
        self.worksheets_gotten = True

    def get_dataframes(self, worksheet=None):
        if worksheet:
            return pd.DataFrame(self.get_worksheets(worksheet_name=worksheet).get_all_records()) 
            
        if not self.worksheets_gotten:
            self.get_worksheets()
        self.gdp_sheet_df = pd.DataFrame(self.gdp_sheet.get_all_records())               
        self.covid_deaths_by_states_df = pd.DataFrame(self.covid_deaths_by_states.get_all_records())
        self.hospital_ratings_df = pd.DataFrame(self.hospital_ratings.get_all_records())
        self.covid_recovery_by_state_df = pd.DataFrame(self.covid_recovery_by_state.get_all_records())
        self.covid_infections_by_states_df = pd.DataFrame(self.covid_infections_by_states.get_all_records())
        self.dataframes_gotten = True
    
    def dataframes(self):
        try:
            self.covid_deaths_by_states_df
        except AttributeError:
            print("Calling DataFrames")
            self.get_dataframes()
            self.covid_deaths_by_states_df
        return {
         'covid_deaths_by_states_df': self.covid_deaths_by_states_df,
         'covid_infections_by_states_df': self.covid_infections_by_states_df,
         'gdp_sheet_df': self.gdp_sheet_df,
         'hospital_ratings_df': self.hospital_ratings_df,
         'covid_recovery_by_state': self.covid_recovery_by_state
         }
        
        
if __name__ == '__main__':
    google_sheets = GoogleSheets()
    google_sheets.get_dataframes()
    dfs = google_sheets.dataframes()
    print(dfs.keys())
    
    

    
    
    
    


    
    
    
    
    