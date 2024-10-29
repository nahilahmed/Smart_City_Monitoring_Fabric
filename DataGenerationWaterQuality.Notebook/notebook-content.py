# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "a124678f-3319-440a-b302-43de78ce38a3",
# META       "default_lakehouse_name": "bronze",
# META       "default_lakehouse_workspace_id": "7a02e1c3-c139-49f8-bd7e-29611cb6a201",
# META       "known_lakehouses": [
# META         {
# META           "id": "a124678f-3319-440a-b302-43de78ce38a3"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

!pip install Faker

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

!pip install kafka-python


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
# Install Faker library if you haven't
# pip install faker

from faker import Faker
import random
import pandas as pd
import numpy as np
import random
import uuid
import json
from datetime import datetime, timedelta

# Initialize Faker
fake = Faker()

# Function to generate fake water quality metrics
def generate_water_quality_data(num_samples):
    data = []

    # Define potential ranges for the metrics
    for _ in range(num_samples):
        sample = {
            'Sample_ID': fake.uuid4(),  # Unique identifier for each sample
            'Date_Collected': fake.date_between(start_date='-1y', end_date='today'),  # Random collection date in the past year
            'Time_Collected': fake.time(),  # Random collection time
            'Location': fake.city(),  # City or location of sample collection
            'pH': round(random.uniform(6.0, 9.0), 2),  # pH levels (optimal between 6-9)
            'Dissolved_Oxygen_mgPerL': round(random.uniform(5.0, 14.0), 2),  # Dissolved oxygen levels in mg/L
            'Turbidity_NTU': round(random.uniform(0.1, 100.0), 2),  # Turbidity (cloudiness) in NTU
            'Conductivity_µSPercm': round(random.uniform(50.0, 1000.0), 2),  # Electrical conductivity in µS/cm
            'Temperature_Degree_C': round(random.uniform(0.0, 35.0), 2),  # Temperature in degrees Celsius
            'Total_Dissolved_Solids_mgPerL': round(random.uniform(50.0, 1200.0), 2),  # TDS in mg/L
            'Ammonia_mgPerL': round(random.uniform(0.0, 5.0), 3),  # Ammonia levels in mg/L (higher indicates pollution)
            'Nitrate_mgPerL': round(random.uniform(0.0, 50.0), 2),  # Nitrate levels in mg/L
            'Phosphate_mgPerL': round(random.uniform(0.0, 2.0), 2),  # Phosphate levels in mg/L
            'Lead_µgPerL': round(random.uniform(0.0, 15.0), 2),  # Lead concentration in µg/L
            'Hardness_mgPerL_as_CaCO3': round(random.uniform(0, 500), 2),  # Water hardness measured as mg/L of CaCO3
            'Sampling_Method': random.choice(['Manual', 'Automatic Sensor']),  # Type of sampling method
            'Inspector_Name': fake.name(),  # Name of the inspector who collected the sample
            'Weather_Conditions': random.choice(['Sunny', 'Rainy', 'Overcast', 'Windy', 'Snowy']),  # Weather during sampling
        }
        data.append(sample)

    # Return the generated data as a DataFrame
    return pd.DataFrame(data)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Function to inject anomalies into water quality data
def inject_anomalies(df, anomaly_percentage=0.8):  # Increased anomaly percentage
    num_anomalies = int(len(df) * anomaly_percentage)
    anomaly_indices = random.sample(range(len(df)), num_anomalies)
    
    for idx in anomaly_indices:
        param_to_modify = random.choice(['pH', 'Dissolved_Oxygen_mgPerL', 'Turbidity_NTU', 
                                         'Conductivity_µSPercm', 'Temperature_Degree_C', 
                                         'Total_Dissolved_Solids_mgPerL', 'Ammonia_mgPerL', 
                                         'Nitrate_mgPerL', 'Phosphate_mgPerL', 'Lead_µgPerL', 
                                         'Hardness_mgPerL_as_CaCO3'])
        
        # Introduce extreme high or low values
        if param_to_modify == 'pH':
            df.at[idx, param_to_modify] = random.choice([random.uniform(0, 1), random.uniform(12, 14)])  # More extreme pH
        elif param_to_modify == 'Dissolved_Oxygen_mgPerL':
            df.at[idx, param_to_modify] = random.choice([random.uniform(0, 0.5), random.uniform(25, 35)])  # Very low DO
        elif param_to_modify == 'Turbidity_NTU':
            df.at[idx, param_to_modify] = random.uniform(400, 1000)  # Extremely high turbidity
        elif param_to_modify == 'Conductivity_µSPercm':
            df.at[idx, param_to_modify] = random.uniform(5000, 10000)  # Extremely high conductivity
        elif param_to_modify == 'Temperature_Degree_C':
            df.at[idx, param_to_modify] = random.choice([random.uniform(-30, 0), random.uniform(50, 70)])  # More extreme temperature
        elif param_to_modify == 'Total_Dissolved_Solids_mgPerL':
            df.at[idx, param_to_modify] = random.uniform(5000, 10000)  # Extremely high TDS
        elif param_to_modify == 'Ammonia_mgPerL':
            df.at[idx, param_to_modify] = random.uniform(15, 30)  # Very high ammonia levels
        elif param_to_modify == 'Nitrate_mgPerL':
            df.at[idx, param_to_modify] = random.uniform(300, 500)  # Extremely high nitrate levels
        elif param_to_modify == 'Phosphate_mgPerL':
            df.at[idx, param_to_modify] = random.uniform(20, 30)  # Extremely high phosphate levels
        elif param_to_modify == 'Lead_µgPerL':
            df.at[idx, param_to_modify] = random.uniform(200, 500)  # Very high lead levels
        elif param_to_modify == 'Hardness_mgPerL_as_CaCO3':
            df.at[idx, param_to_modify] = random.uniform(3000, 5000)  # Extremely hard water

    return df


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************



# Function to generate poor quality water data
def generate_poor_quality_data(num_samples=50):
    data = []
    base_date = datetime.now() - timedelta(days=num_samples)  # Base date for collection
    
    for i in range(num_samples):
        sample = {
            'Sample_ID': str(uuid.uuid4()),
            'Date_Collected': int((base_date + timedelta(days=i)).timestamp() * 1000),  # milliseconds timestamp
            'Time_Collected': (datetime.now() + timedelta(minutes=random.randint(0, 1440))).strftime("%H:%M:%S"),
            'Location': random.choice(['North River', 'South Creek', 'East Stephanie', 'West Brook']),
            'pH': random.uniform(2, 3),  # Very acidic
            'Dissolved_Oxygen_mgPerL': random.uniform(0.5, 2),  # Critically low DO
            'Turbidity_NTU': random.uniform(300, 500),  # Very high turbidity
            'Conductivity_µSPercm': random.uniform(4000, 5000),  # Extremely high conductivity
            'Temperature_Degree_C': random.uniform(40, 50),  # High temperature
            'Total_Dissolved_Solids_mgPerL': random.uniform(4500, 5500),  # High TDS
            'Ammonia_mgPerL': random.uniform(7, 10),  # Dangerous ammonia level
            'Nitrate_mgPerL': random.uniform(100, 200),  # High nitrate level
            'Phosphate_mgPerL': random.uniform(8, 15),  # High phosphate
            'Lead_µgPerL': random.uniform(70, 100),  # Unsafe lead level
            'Hardness_mgPerL_as_CaCO3': random.uniform(1800, 2500),  # Extremely hard water
            'Sampling_Method': random.choice(['Manual', 'Automated']),
            'Inspector_Name': random.choice(['Shelley Brown', 'John Doe', 'Alice Smith', 'Bob Williams']),
            'Weather_Conditions': random.choice(['Snowy', 'Stormy', 'Foggy'])
        }
        data.append(sample)
    
    return pd.DataFrame(data)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

water_q_d=generate_water_quality_data(100)
poor_Q_d=generate_poor_quality_data(50)
water_quality_data = pd.concat([water_q_d, poor_Q_d])
shapeOfData=water_quality_data.shape[0]
water_quality_data=json.loads(water_quality_data.to_json(orient="records"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# from kafka import KafkaProducer
# from kafka.errors import KafkaError
# import time
# import os

# NUM_MESSAGES=150
# connectionString = "Endpoint=sb://esehpnbzwczb8pa5un96dt.servicebus.windows.net/;SharedAccessKeyName=key_27fa3ef7-7c13-26c4-0742-094657f3bac2;SharedAccessKey=IRwi+7yhY1QmUw+UMxyrB4tIf7Suy/oVl+AEhI8Li9E=;EntityPath=es_2549e6b5-f1cc-4118-89ae-e1430741a85e"
# eventHubName = "es_2549e6b5-f1cc-4118-89ae-e1430741a85e"

# def get_properties():
#     # Extract namespace from connection string
#     namespace = connectionString[connectionString.index("/") + 2: connectionString.index(".")]
    
#     # Build the producer config properties
#     config = {
#         'bootstrap_servers': f"{namespace}.servicebus.windows.net:9093",
#         'security_protocol': 'SASL_SSL',
#         'sasl_mechanism': 'PLAIN',
#         'sasl_plain_username': '$ConnectionString',
#         'sasl_plain_password': connectionString,
#         'client_id': 'KafkaExampleProducer',
#         'key_serializer': lambda x: x.to_bytes(8, 'big'),
#         'value_serializer': lambda x: str(x).encode('utf-8')
#     }
#     return config

# def publish_events(producer):
#     for i in range(NUM_MESSAGES):
#         current_time = int(time.time() * 1000)
#         jsonData=water_quality_data[i]
#         jsonData['LoadDateTime']=current_time
#         # Sending the message
#         future = producer.send(eventHubName, key=current_time, value=jsonData)
        
#         try:
#             record_metadata = future.get(timeout=10)
#         except KafkaError as e:
#             print(e)
#             break
    
#     print(f"Sent {NUM_MESSAGES} messages")

# if __name__ == "__main__":
# #     # Get producer properties
#     producer_config = get_properties()
    
# #     # Initialize Kafka Producer
#     producer = KafkaProducer(**producer_config)
    
# #     # Send messages to Kafka
#     publish_events(producer)
    
# #     # Close the producer
#     producer.close()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM bronze.bronze_waterQuality LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
