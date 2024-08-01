import pandas as pd
import requests
import json
import os
from sqlalchemy import create_engine

api_url = os.environ.get("API_URL")
#print(api_url)
payload = json.dumps({"type":["CURRENT"]})
headers = {'Content-Type':'application/json'}

db_url = os.getenv("DB_URL")
#print(db_url)
engine = create_engine(db_url)

response = requests.request("POST",api_url,headers=headers,data=payload)
data = response.json()

fuel_types = pd.DataFrame(data["FUEL_CURRENT"])
fuel_types.to_sql('fuel_types', con=engine, if_exists='replace', index=False)
print(fuel_types.head())
print("Successfully stored fuel types data in database.")