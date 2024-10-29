# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

!pip install folium

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Import necessary libraries
import folium
from geopy.distance import geodesic
import pandas as pd
from itertools import permutations

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#This is an example of Reading data from Kusto. Replace T with your <tablename>
kustoQuery = """
smartcitywastedata
| where occupancyPercentage >= 90
| extend latitude = todouble(location.lat), longitude = todouble(location.lon)
| project binID, latitude, longitude, occupancyPercentage
| take 5
"""

# The Kusto cluster uri to read the data. The query Uri is of the form https://<>.kusto.data.microsoft.com 
kustoUri = "https://trd-pkce15jwwtnekncq6t.z0.kusto.fabric.microsoft.com"
# The database to read the data
database = "smartcitywasterflow-eh"
# The access credentials for the write
accessToken = mssparkutils.credentials.getToken(kustoUri)
kustoDf  = spark.read\
            .format("com.microsoft.kusto.spark.synapse.datasource")\
            .option("accessToken", accessToken)\
            .option("kustoCluster", kustoUri)\
            .option("kustoDatabase", database) \
            .option("kustoQuery", kustoQuery).load()

display(kustoDf)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Convert the list of dictionaries into a DataFrame for easier handling
df_bins = kustoDf.toPandas()

# Function to calculate total distance of a path
def calculate_total_distance(path, df):
    total_distance = 0
    for i in range(len(path) - 1):
        loc1 = (df.loc[path[i], 'latitude'], df.loc[path[i], 'longitude'])
        loc2 = (df.loc[path[i+1], 'latitude'], df.loc[path[i+1], 'longitude'])
        total_distance += geodesic(loc1, loc2).kilometers
    return total_distance

# Find the optimal route (shortest path) for visiting all bins
bin_indices = df_bins.index.tolist()
all_paths = permutations(bin_indices)
optimal_path = min(all_paths, key=lambda x: calculate_total_distance(x, df_bins))
optimal_path = list(optimal_path) + [optimal_path[0]]  # Return to starting point

# Initialize a map centered around the average location of the bins
avg_lat = df_bins['latitude'].mean()
avg_lon = df_bins['longitude'].mean()
route_map = folium.Map(location=[avg_lat, avg_lon], zoom_start=14)

# Add bin markers to the map
for i, row in df_bins.iterrows():
    folium.Marker(
        location=[row['latitude'], row['longitude']],
        popup=row['binID'],
        tooltip=row['binID'],
        icon=folium.Icon(color="blue", icon="info-sign"),
    ).add_to(route_map)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Draw lines between bins to show the route
for i in range(len(optimal_path) - 1):
    loc1 = [df_bins.loc[optimal_path[i], 'latitude'], df_bins.loc[optimal_path[i], 'longitude']]
    loc2 = [df_bins.loc[optimal_path[i + 1], 'latitude'], df_bins.loc[optimal_path[i + 1], 'longitude']]
    folium.PolyLine([loc1, loc2], color="red", weight=2.5, opacity=1).add_to(route_map)

# Display the map
route_map

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
