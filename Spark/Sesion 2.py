# Databricks notebook source
# MAGIC %run ../Includes/Classroom-Setup-00.02L

# COMMAND ----------

events_df=spark.table("events")

# COMMAND ----------

display(events_df)

# COMMAND ----------

mac_df=events_df.orderBy("event_timestamp").where("device == 'macOS'")

# COMMAND ----------

n_row=mac_df.count()

head=mac_df.head()

rows=mac_df.take(5)

# COMMAND ----------

# Usando una query

mac_sql_df = spark.sql("select * from events where device='macOS' order by event_timestamp")

display(mac_sql_df)

# COMMAND ----------

DA.cleanup()

# COMMAND ----------


