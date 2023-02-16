# Databricks notebook source
# MAGIC %md # Sesion 6

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup-00.06L

# COMMAND ----------

from pyspark.sql.functions import col

# Purchase events logged on the BedBricks website
df = (spark.table("events")
      .withColumn("revenue", col("ecommerce.purchase_revenue_in_usd"))
      .filter(col("revenue").isNotNull())
      .drop("event_name")
     )

display(df)

# COMMAND ----------

# MAGIC %md ### 1. Aggregate revenue by traffic source
# MAGIC - Group by **`traffic_source`**
# MAGIC - Get sum of **`revenue`** as **`total_rev`**. Round this to the tens decimal place (e.g. `nnnnn.n`). 
# MAGIC - Get average of **`revenue`** as **`avg_rev`**
# MAGIC 
# MAGIC Remember to import any necessary built-in functions.

# COMMAND ----------

from pyspark.sql.functions import col,round
from pyspark.sql.functions import sum,avg

traffic_df = (df.groupBy('traffic_source')
              .agg(sum(round(col('revenue'),1)).alias('total_rev')
                  ,avg(col('revenue')).alias('avg_rev'))
             )
            
              
display(traffic_df)

# COMMAND ----------

from pyspark.sql.functions import round

expected1 = [(620096.0, 1049.2318), (4026578.5, 986.1814), (1200591.0, 1067.192), (2322856.0, 1093.1087), (826921.0, 1086.6242), (404911.0, 1091.4043)]
test_df = traffic_df.sort("traffic_source").select(round("total_rev", 4).alias("total_rev"), round("avg_rev", 4).alias("avg_rev"))
result1 = [(row.total_rev, row.avg_rev) for row in test_df.collect()]

assert(expected1 == result1)
print("All test pass")

# COMMAND ----------

# MAGIC %md ### 2. Get top three traffic sources by total revenue
# MAGIC - Sort by **`total_rev`** in descending order
# MAGIC - Limit to first three rows

# COMMAND ----------



top_traffic_df = (traffic_df.sort(col('total_rev').desc()).limit(3)
)
display(top_traffic_df)

# COMMAND ----------

expected2 = [(4026578.5, 986.1814), (2322856.0, 1093.1087), (1200591.0, 1067.192)]
test_df = top_traffic_df.select(round("total_rev", 4).alias("total_rev"), round("avg_rev", 4).alias("avg_rev"))
result2 = [(row.total_rev, row.avg_rev) for row in test_df.collect()]

assert(expected2 == result2)
print("All test pass")

# COMMAND ----------

# MAGIC %md ### 3. Limit revenue columns to two decimal places
# MAGIC - Modify columns **`avg_rev`** and **`total_rev`** to contain numbers with two decimal places
# MAGIC   - Use **`withColumn()`** with the same names to replace these columns
# MAGIC   - To limit to two decimal places, multiply each column by 100, cast to long, and then divide by 100

# COMMAND ----------

final_df = (top_traffic_df.withColumn('avg_rev',(100*col('avg_rev')).cast('long')/100).withColumn('total_rev',round('total_rev',2))
)

display(final_df)

# COMMAND ----------

expected3 = [(4026578.5, 986.18), (2322856.0, 1093.1), (1200591.0, 1067.19)]
result3 = [(row.total_rev, row.avg_rev) for row in final_df.collect()]

assert(expected3 == result3)
print("All test pass")

# COMMAND ----------

# MAGIC %md ### 5. Chain all the steps above

# COMMAND ----------

chain_df = (df.groupBy('traffic_source')
              .agg(sum(round(col('revenue'),1)).alias('total_rev')
                  ,avg(col('revenue')).alias('avg_rev'))
                .sort(col('total_rev').desc()).limit(3)
            .withColumn('avg_rev',round(col('avg_rev'),2)).withColumn('total_rev',round('total_rev',2))
)

display(chain_df)

# COMMAND ----------

DA.cleanup()

# COMMAND ----------


