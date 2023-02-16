# Databricks notebook source
# MAGIC %run ../Includes/Classroom-Setup-00.04L

# COMMAND ----------

# MAGIC %md ### 1. Extract purchase revenue for each event
# MAGIC Add new column **`revenue`** by extracting **`ecommerce.purchase_revenue_in_usd`**

# COMMAND ----------

events_df=table("events")

# COMMAND ----------

revenue_df = events_df.withColumn('revenue',events_df.ecommerce["purchase_revenue_in_usd"])
display(revenue_df)

# COMMAND ----------

# comprobando los resultados

from pyspark.sql.functions import col
expected1 = [4351.5, 4044.0, 3985.0, 3946.5, 3885.0, 3590.0, 3490.0, 3451.5, 3406.5, 3385.0]
result1 = [row.revenue for row in revenue_df.sort(col("revenue").desc_nulls_last()).limit(10).collect()]
print(result1)
assert(expected1 == result1)
print("All test pass")

# COMMAND ----------

# MAGIC %md ### 2. Filter events where revenue is not null
# MAGIC Filter for records where **`revenue`** is not **`null`**

# COMMAND ----------



purchases_df = revenue_df.filter(revenue_df.revenue.isNotNull())

display(purchases_df)

# COMMAND ----------

assert purchases_df.filter(col("revenue").isNull()).count() == 0, "Nulls in 'revenue' column"
print("All test pass")

# COMMAND ----------

# MAGIC %md ### 3. Check what types of events have revenue
# MAGIC Find unique **`event_name`** values in **`purchases_df`** in one of two ways:
# MAGIC - Select "event_name" and get distinct records
# MAGIC - Drop duplicate records based on the "event_name" only
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/icon_hint_32.png" alt="Hint"> There's only one event associated with revenues

# COMMAND ----------

distinct_df = purchases_df.select("distinct event_name")
display(distinct_df)

# COMMAND ----------


