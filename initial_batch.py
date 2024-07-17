## Australian Energy Production Data Pipeline
import pandas as pd
import requests
import json
import os
from sqlalchemy import create_engine

api_url = "https://visualisations.aemo.com.au/aemo/apps/api/report/5MIN"
payload = json.dumps({"timeScale":["5MIN"]})
headers = {'Content-Type':'application/json'}

db_url = os.getenv("DB_URL")
engine = create_engine(db_url)

response = requests.request("POST",api_url,headers=headers,data=payload)
data = response.json()

eGen = pd.DataFrame(data["5MIN"])

state_dfs = {state: pd.DataFrame(columns=eGen.columns) for state in eGen['REGION'].unique()}
for state in state_dfs.keys():
    state_dfs[state] = eGen[eGen['REGION'] == state]
    state_dfs[state].to_sql(f'electricity_data_{state}', con=engine, if_exists='append', index=False)


