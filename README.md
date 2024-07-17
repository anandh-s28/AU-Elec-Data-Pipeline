# AEMO Electricity Generation Data Pipeline

This is a data pipeline created for streaming state-wise electricity data from AEMO to a PostgreSQL database (Supabase) using Kafka.

## Feature

- Live data updates: The producer fetches data from AEMO's API and updates the corresponding state's table in the database once every 5 minutes.

## Installation

1. Clone the repo
2. Install the required dependencies from the requirements.txt file

## Usage

- Store your Postgres URI in a .env file as "DB_URL"
- Spin up an AWS EC2 instance (Ubuntu). Install Kafka and Java.
- Configure Kafka in the EC2 instance. Uncomment 'advertised_listeners' and add your EC2 instance's public IP address here. Start Zookeeper and Kafka server.
- In the instance's security rules, allow incoming traffic from your IP address only. Then, start your producer and consumer.
- Replace the IP address here with your instance's IP address.
- Run the initial_batch.py script to fetch the first batch of data into your database.
- Run the producer and consumer scripts for updates.
