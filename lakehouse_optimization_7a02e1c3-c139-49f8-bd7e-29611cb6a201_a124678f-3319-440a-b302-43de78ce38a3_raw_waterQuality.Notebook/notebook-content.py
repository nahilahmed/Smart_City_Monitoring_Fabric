# Fabric notebook source

# METADATA ********************

# META {
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "a124678f-3319-440a-b302-43de78ce38a3",
# META       "default_lakehouse_name": "bronze",
# META       "default_lakehouse_workspace_id": "7a02e1c3-c139-49f8-bd7e-29611cb6a201",
# META       "known_lakehouses": [
# META         {
# META           "id": "a124678f-3319-440a-b302-43de78ce38a3",
# META           "name": "bronze"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Run the below script or schedule it to run regularly to optimize your Lakehouse table 'raw_waterQuality'

from delta.tables import *
deltaTable = DeltaTable.forName(spark, "raw_waterQuality")
deltaTable.optimize().executeCompaction()

# If you only want to optimize a subset of your data, you can specify an optional partition predicate. For example:
#
#     from datetime import datetime, timedelta
#     startDate = (datetime.now() - timedelta(days=3)).strftime('%Y-%m-%d')
#     deltaTable.optimize().where("date > '{}'".format(startDate)).executeCompaction()

