-- Fabric notebook source

-- METADATA ********************

-- META {
-- META   "kernel_info": {
-- META     "name": "synapse_pyspark"
-- META   },
-- META   "dependencies": {
-- META     "lakehouse": {
-- META       "default_lakehouse": "8ff9da31-76e3-417f-9b47-2616293fec0c",
-- META       "default_lakehouse_name": "Lakehouse",
-- META       "default_lakehouse_workspace_id": "959bf169-7bd5-4462-8070-0dcab9eb4d7e",
-- META       "known_lakehouses": [
-- META         {
-- META           "id": "8ff9da31-76e3-417f-9b47-2616293fec0c"
-- META         }
-- META       ]
-- META     }
-- META   }
-- META }

-- CELL ********************

SELECT * FROM Lakehouse.silver.taxi_trips LIMIT 1000

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************


-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }
