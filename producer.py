from kafka import KafkaProducer
from json import dumps
import pandas as pd
import json
import requests
import time


producer = KafkaProducer(bootstrap_servers=['44.204.30.120:9092'], 
                         value_serializer=lambda x: 
                         dumps(x).encode('utf-8'))




def fetch_and_send_to_kafka():
    """
    Fetches data from an API endpoint, processes it, and sends it to a Kafka topic.

    This function makes a POST request to the specified API endpoint to fetch data. The fetched data is then processed
    to extract the latest rows for each region. Each extracted row is converted to a dictionary and sent to a Kafka
    topic using a KafkaProducer.

    Returns:
        None

    Raises:
        None
    """
    api_url = "https://visualisations.aemo.com.au/aemo/apps/api/report/5MIN"
    payload = json.dumps({"timeScale":["5MIN"]})
    headers = {'Content-Type':'application/json'}
    while True:
        
        response = requests.request("POST",api_url,headers=headers,data=payload)
        new_data = response.json()
        new_data_5min = new_data["5MIN"]
        new_df = pd.DataFrame(new_data_5min)
        
        latest_rows = new_df.groupby('REGION').tail(1)
        
        for _, record in latest_rows.iterrows():
            producer.send('aemo_pipeline', value=record.to_dict())
        
        print("Sleeping for 5 minutes...")
        time.sleep(300)
        
fetch_and_send_to_kafka()