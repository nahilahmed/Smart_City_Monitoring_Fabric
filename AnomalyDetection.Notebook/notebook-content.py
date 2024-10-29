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

!pip install kafka-python

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

water_quality_data = spark.sql("""SELECT Sample_ID, Date_Collected, Time_Collected, Location, pH,
       Dissolved_Oxygen_mgPerL, Turbidity_NTU, `Conductivity_µSPercm`,
       Temperature_Degree_C, Total_Dissolved_Solids_mgPerL,
       Ammonia_mgPerL, Nitrate_mgPerL, Phosphate_mgPerL,`Lead_µgPerL`,
       Hardness_mgPerL_as_CaCO3, Sampling_Method, Inspector_Name,
       Weather_Conditions
 FROM bronze.bronze_waterQuality""").toPandas()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import pandas as pd

def anomalyFlag(x, threshold):
    if x > threshold or x < -threshold:
        return 1
    return 0

# Function to calculate z-scores and detect anomalies efficiently
def z_score_anomaly_detection(df, threshold=1.5):
    zscore_dict = {}
    anomaly_dict = {}
    
    columns = df.columns
    for col in columns:
        if (df[col].dtype == 'float64') or (df[col].dtype == 'int64'):  # Only apply to numerical columns
            mean = df[col].mean()
            std = df[col].std()

            # Calculate z-scores
            zscore_col = (df[col] - mean) / std
            zscore_dict[f'zscore_{col}'] = zscore_col

            # Calculate anomalies based on z-scores
            anomaly_col = zscore_col.apply(lambda x: anomalyFlag(x, threshold))
            anomaly_dict[f'anomaly__{col}'] = anomaly_col

    # Combine all z-score and anomaly columns at once using pd.concat
    zscore_df = pd.DataFrame(zscore_dict)
    anomaly_df = pd.DataFrame(anomaly_dict)

    # Concatenate the new columns back to the original DataFrame
    df = pd.concat([df, zscore_df, anomaly_df], axis=1)
    return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# # Detect anomalies using z-scores
water_quality_data_with_anomalies = z_score_anomaly_detection(water_quality_data)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Example weights for each parameter (adjust based on importance)
weights = {
    'pH': 0.1,
    'Dissolved_Oxygen_mgPerL': 0.2,  # Increased weight
    'Turbidity_NTU': 0.1,
    'Conductivity_µSPercm': 0.05,
    'Temperature_Degree_C': 0.05,
    'Total_Dissolved_Solids_mgPerL': 0.1,
    'Ammonia_mgPerL': 0.15,  # Increased weight
    'Nitrate_mgPerL': 0.1,
    'Phosphate_mgPerL': 0.1,
    'Lead_µgPerL': 0.1,
    'Hardness_mgPerL_as_CaCO3': 0.05
}


# Function to convert z-score to score (0-100 scale)
def zscore_to_score(zscore):
    if abs(zscore) <= 1:
        return 100  # Excellent
    elif abs(zscore) <= 1.5:
        return 75  # Good
    elif abs(zscore) <= 2:
        return 50  # Moderate
    elif abs(zscore) <= 3:
        return 25  # Poor
    else:
        return 0  # Very Poor


# Calculate the WQHI for each sample
water_quality_data_with_anomalies['WQHI'] = (
    weights['pH'] * water_quality_data_with_anomalies['zscore_pH'].apply(zscore_to_score) +
    weights['Dissolved_Oxygen_mgPerL'] * water_quality_data_with_anomalies['zscore_Dissolved_Oxygen_mgPerL'].apply(zscore_to_score) +
    weights['Turbidity_NTU'] * water_quality_data_with_anomalies['zscore_Turbidity_NTU'].apply(zscore_to_score) +
    weights['Conductivity_µSPercm'] * water_quality_data_with_anomalies['zscore_Conductivity_µSPercm'].apply(zscore_to_score) +
    weights['Temperature_Degree_C'] * water_quality_data_with_anomalies['zscore_Temperature_Degree_C'].apply(zscore_to_score) +
    weights['Total_Dissolved_Solids_mgPerL'] * water_quality_data_with_anomalies['zscore_Total_Dissolved_Solids_mgPerL'].apply(zscore_to_score) +
    weights['Ammonia_mgPerL'] * water_quality_data_with_anomalies['zscore_Ammonia_mgPerL'].apply(zscore_to_score) +
    weights['Nitrate_mgPerL'] * water_quality_data_with_anomalies['zscore_Nitrate_mgPerL'].apply(zscore_to_score) +
    weights['Phosphate_mgPerL'] * water_quality_data_with_anomalies['zscore_Phosphate_mgPerL'].apply(zscore_to_score) +
    weights['Lead_µgPerL'] * water_quality_data_with_anomalies['zscore_Lead_µgPerL'].apply(zscore_to_score) +
    weights['Hardness_mgPerL_as_CaCO3'] * water_quality_data_with_anomalies['zscore_Hardness_mgPerL_as_CaCO3'].apply(zscore_to_score)
) / sum(weights.values())


# Function to categorize WQHI into qualitative descriptions
def categorize_wqhi(wqhi):
    if wqhi >= 90:
        return 'Excellent'
    elif wqhi >= 70:
        return 'Good'
    elif wqhi >= 50:
        return 'Moderate'
    elif wqhi >= 30:
        return 'Poor'
    else:
        return 'Very Poor'

def flag_for_processing(water_quality):
    if water_quality in ['Moderate', 'Poor', 'Very Poor']:
        return "True"
    else:
        return "False"
# Add a new column 'Water_Quality' based on the WQHI
water_quality_data_with_anomalies['Water_Quality'] = water_quality_data_with_anomalies['WQHI'].apply(categorize_wqhi)
# Function to flag samples that need further processing
water_quality_data_with_anomalies['Needs_Processing'] = water_quality_data_with_anomalies['Water_Quality'].apply(flag_for_processing)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# water_quality_data_with_anomalies['Water_Quality'].unique(),water_quality_data_with_anomalies['Needs_Processing'].unique()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# print(water_quality_data_with_anomalies[['WQHI']].describe())


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json

# Convert the DataFrame to JSON string
water_quality_data_json = water_quality_data_with_anomalies.to_json(orient="records")
# Load the JSON string back as a Python list of dictionaries
water_quality_data_with_anomalies_json = json.loads(water_quality_data_json)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from kafka import KafkaProducer
from kafka.errors import KafkaError
import time
import os

NUM_MESSAGES=water_quality_data_with_anomalies.shape[0]
connectionString = "Endpoint=sb://esehpn28uwytd9hrhmcwkl.servicebus.windows.net/;SharedAccessKeyName=key_fdaa9814-ccca-c295-a11d-6824a8799965;SharedAccessKey=Wz3HNGjYR5H+qt1vpIm9sy197Jjf50vkY+AEhPeFgk0=;EntityPath=es_ac48eea1-e065-4f99-afa7-500cc3868b65"
eventHubName = "es_ac48eea1-e065-4f99-afa7-500cc3868b65"

def get_properties():
    # Extract namespace from connection string
    namespace = connectionString[connectionString.index("/") + 2: connectionString.index(".")]
    
    # Build the producer config properties
    config = {
        'bootstrap_servers': f"{namespace}.servicebus.windows.net:9093",
        'security_protocol': 'SASL_SSL',
        'sasl_mechanism': 'PLAIN',
        'sasl_plain_username': '$ConnectionString',
        'sasl_plain_password': connectionString,
        'client_id': 'KafkaExampleProducer',
        'key_serializer': lambda x: x.to_bytes(8, 'big'),
        'value_serializer': lambda x: str(x).encode('utf-8')
    }
    return config

def publish_events(producer):
    for i in range(NUM_MESSAGES):
        current_time = int(time.time() * 1000)
        jsonData=water_quality_data_with_anomalies_json[i]
        jsonData['LoadDateTime']=current_time
        jsonData=json.dumps(jsonData)
        # Sending the message
        future = producer.send(eventHubName, key=current_time, value=jsonData)
        
        try:
            record_metadata = future.get(timeout=10)
        except KafkaError as e:
            print(e)
            break
    
    print(f"Sent {NUM_MESSAGES} messages")

if __name__ == "__main__":
#     # Get producer properties
    producer_config = get_properties()
    
#     # Initialize Kafka Producer
    producer = KafkaProducer(**producer_config)
    
#     # Send messages to Kafka
    publish_events(producer)
    
#     # Close the producer
    producer.close()


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
