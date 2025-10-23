# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "f1f004f2-519c-4836-ae81-684144655fa0",
# META       "default_lakehouse_name": "lkh_source",
# META       "default_lakehouse_workspace_id": "de5ec295-1bf8-49f8-b31b-b1dfed35a224",
# META       "known_lakehouses": [
# META         {
# META           "id": "f1f004f2-519c-4836-ae81-684144655fa0"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

df = spark.sql('''
    WITH
    t0(i) AS (SELECT 0 UNION ALL SELECT 0), --             2 rows
    t1(i) AS (SELECT 0 FROM t0 a, t0 b),    --             4 rows
    t2(i) AS (SELECT 0 FROM t1 a, t1 b),    --            16 rows
    t3(i) AS (SELECT 0 FROM t2 a, t2 b),    --           256 rows
    --t4(i) AS (SELECT 0 FROM t3 a, t3 b),  --        65,536 rows
    --t5(i) AS (SELECT 0 FROM t4 a, t4 b),  -- 4,294,967,296 rows

    n(i) AS (SELECT ROW_NUMBER() OVER(ORDER BY (SELECT 0)) FROM t3)
    
    SELECT i, now() as InsertedDT
    FROM n 
    WHERE i BETWEEN 1 AND 100
''')

display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.write.mode("overwrite").saveAsTable("fake_data");

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC SELECT * FROM fake_data;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
