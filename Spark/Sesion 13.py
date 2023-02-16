# Databricks notebook source
# MAGIC %md # Sesion 13

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup-00.13

# COMMAND ----------

df = spark.read.table("events")
display(df)

# COMMAND ----------

# MAGIC %md ### Logical Optimization
# MAGIC 
# MAGIC **`explain(..)`** prints the query plans, optionally formatted by a given explain mode. Compare the following logical plan & physical plan, noting how Catalyst handled the multiple **`filter`** transformations.

# COMMAND ----------

from pyspark.sql.functions import col

limit_events_df = (df
                   .filter(col("event_name") != "reviews")
                   .filter(col("event_name") != "checkout")
                   .filter(col("event_name") != "register")
                   .filter(col("event_name") != "email_coupon")
                   .filter(col("event_name") != "cc_info")
                   .filter(col("event_name") != "delivery")
                   .filter(col("event_name") != "shipping_info")
                   .filter(col("event_name") != "press")
                  )

limit_events_df.explain(True)

# COMMAND ----------

# Antes ha optimizado el para ponerlo todo en un solo filtro, ahora se lo pasamos nosotros directamente

better_df = (df
             .filter((col("event_name").isNotNull()) &
                     (col("event_name") != "reviews") &
                     (col("event_name") != "checkout") &
                     (col("event_name") != "register") &
                     (col("event_name") != "email_coupon") &
                     (col("event_name") != "cc_info") &
                     (col("event_name") != "delivery") &
                     (col("event_name") != "shipping_info") &
                     (col("event_name") != "press"))
            )

better_df.explain(True)

# COMMAND ----------

stupid_df = (df
             .filter(col("event_name") != "finalize")
             .filter(col("event_name") != "finalize")
             .filter(col("event_name") != "finalize")
             .filter(col("event_name") != "finalize")
             .filter(col("event_name") != "finalize")
            )

stupid_df.explain(True)

# COMMAND ----------

DA.cleanup()

# COMMAND ----------


