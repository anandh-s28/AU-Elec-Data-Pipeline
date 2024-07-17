from kafka import KafkaConsumer
import os
from json import loads
from sqlalchemy import create_engine
import pandas as pd

def consume_and_store_data():
    """
    Consumes data from a Kafka topic and stores it in a PostgreSQL database.

    This function creates a Kafka consumer that consumes messages from the 'aemo_pipeline' topic.
    It deserializes the message values using JSON and stores the data in a DataFrame.
    The 'REGION' field from each record is used as the table name to store the data in the database.
    The data is then stored in a PostgreSQL database using SQLAlchemy.

    Args:
        None

    Returns:
        None
    """
    consumer = KafkaConsumer('aemo_pipeline', bootstrap_servers='44.204.30.120:9092', value_deserializer=lambda v: loads(v.decode('utf-8')))
    db_url = os.getenv("DB_URL")
    engine = create_engine(db_url)

    for message in consumer:
        record = message.value
        df = pd.DataFrame([record])
        state = record['REGION']
        print(state)
        df.to_sql(f'electricity_data_{state}', con=engine, if_exists='append', index=False)
