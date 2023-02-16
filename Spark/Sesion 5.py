# Databricks notebook source
# MAGIC %md # Sesion 5
# MAGIC Agregaciones

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup-00.05

# COMMAND ----------

df = spark.table("events")
display(df)

# COMMAND ----------

df.groupBy("event_name")

# COMMAND ----------

df_co=df.groupBy("event_name").count()
display(df_co)

# COMMAND ----------

avg_state_purchases_df = df.groupBy("geo.state").avg("ecommerce.purchase_revenue_in_usd")
display(avg_state_purchases_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC Existen muchos métodos conocidos como min,max,avg,count,mean,pivot,sum..

# COMMAND ----------

city_purchase_quantities_df = df.groupBy("geo.state", "geo.city").sum("ecommerce.total_item_quantity", "ecommerce.purchase_revenue_in_usd")
display(city_purchase_quantities_df)

# COMMAND ----------

# MAGIC %md ### Aggregate Functions
# MAGIC 
# MAGIC Here are some of the built-in functions available for aggregation.
# MAGIC 
# MAGIC | Method | Description |
# MAGIC | --- | --- |
# MAGIC | approx_count_distinct | Returns the approximate number of distinct items in a group |
# MAGIC | avg | Returns the average of the values in a group |
# MAGIC | collect_list | Returns a list of objects with duplicates |
# MAGIC | corr | Returns the Pearson Correlation Coefficient for two columns |
# MAGIC | max | Compute the max value for each numeric columns for each group |
# MAGIC | mean | Compute the average value for each numeric columns for each group |
# MAGIC | stddev_samp | Returns the sample standard deviation of the expression in a group |
# MAGIC | sumDistinct | Returns the sum of distinct values in the expression |
# MAGIC | var_pop | Returns the population variance of the values in a group |
# MAGIC 
# MAGIC Use the grouped data method <a href="https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.GroupedData.agg.html#pyspark.sql.GroupedData.agg" target="_blank">**`agg`**</a> to apply built-in aggregate functions
# MAGIC 
# MAGIC This allows you to apply other transformations on the resulting columns, such as <a href="https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.Column.alias.html" target="_blank">**`alias`**</a>.

# COMMAND ----------

from pyspark.sql.functions import sum

state_purchases_df = df.groupBy("geo.state").agg(sum("ecommerce.total_item_quantity").alias("total_purchases"))
display(state_purchases_df)

# COMMAND ----------

from pyspark.sql.functions import avg, approx_count_distinct

state_aggregates_df = (df
                       .groupBy("geo.state")
                       .agg(avg("ecommerce.total_item_quantity").alias("avg_quantity"),
                            approx_count_distinct("user_id").alias("distinct_users"))
                      )

display(state_aggregates_df)

# COMMAND ----------

# MAGIC %md ### Math Functions
# MAGIC Here are some of the built-in functions for math operations.
# MAGIC 
# MAGIC | Method | Description |
# MAGIC | --- | --- |
# MAGIC | ceil | Computes the ceiling of the given column. |
# MAGIC | cos | Computes the cosine of the given value. |
# MAGIC | log | Computes the natural logarithm of the given value. |
# MAGIC | round | Returns the value of the column e rounded to 0 decimal places with HALF_UP round mode. |
# MAGIC | sqrt | Computes the square root of the specified float value. |

# COMMAND ----------

from pyspark.sql.functions import cos, sqrt

display(spark.range(10)  # Create a DataFrame with a single column called "id" with a range of integer values
        .withColumn("sqrt", sqrt("id"))
        .withColumn("cos", cos("id"))
       )

# COMMAND ----------

DA.cleanup()

# COMMAND ----------


