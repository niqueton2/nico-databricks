# Databricks notebook source
# MAGIC %run ../Includes/Classroom-Setup-00.03

# COMMAND ----------

events_df = spark.table("events")
display(events_df)

# COMMAND ----------

from pyspark.sql.functions import col

print(events_df.device)
print(events_df["device"])
print(col("device"))

# COMMAND ----------

col("ecommerce.purchase_revenue_in_usd") + col("ecommerce.total_item_quantity")
col("event_timestamp").desc()
(col("ecommerce.purchase_revenue_in_usd") * 100).cast("int")

# COMMAND ----------

rev_df = (events_df
         .filter(col("ecommerce.purchase_revenue_in_usd").isNotNull())
         .withColumn("purchase_revenue", (col("ecommerce.purchase_revenue_in_usd") * 100).cast("int"))
         .withColumn("avg_purchase_revenue", col("ecommerce.purchase_revenue_in_usd") / col("ecommerce.total_item_quantity"))
         .sort(col("avg_purchase_revenue").desc())
        )

display(rev_df)

# COMMAND ----------

# MAGIC %md ## DataFrame Transformation Methods
# MAGIC | Method | Description |
# MAGIC | --- | --- |
# MAGIC | **`select`** | Returns a new DataFrame by computing given expression for each element |
# MAGIC | **`drop`** | Returns a new DataFrame with a column dropped |
# MAGIC | **`withColumnRenamed`** | Returns a new DataFrame with a column renamed |
# MAGIC | **`withColumn`** | Returns a new DataFrame by adding a column or replacing the existing column that has the same name |
# MAGIC | **`filter`**, **`where`** | Filters rows using the given condition |
# MAGIC | **`sort`**, **`orderBy`** | Returns a new DataFrame sorted by the given expressions |
# MAGIC | **`dropDuplicates`**, **`distinct`** | Returns a new DataFrame with duplicate rows removed |
# MAGIC | **`limit`** | Returns a new DataFrame by taking the first n rows |
# MAGIC | **`groupBy`** | Groups the DataFrame using the specified columns, so we can run aggregation on them |

# COMMAND ----------

# Para poner alias en el select utilizamos col

from pyspark.sql.functions import col

locations_df = events_df.select(
    "user_id", 
    col("geo.city").alias("city"), 
    col("geo.state").alias("state")
)
display(locations_df)

# COMMAND ----------

# Con el selectExpr podemos crear otra columna dependiente de cierta condición por ejemplo

apple_df = events_df.selectExpr("user_id", "device in ('macOS', 'iOS') as apple_user")
display(apple_df)

# COMMAND ----------

# Con drop podemos elimina ciertas columnas de un dataframe, puede venir bien si queremos hacer un select de la myoría de los campos

anonymous_df = events_df.drop("user_id", "geo", "device")
display(anonymous_df)

# COMMAND ----------

# Con el withcolumn podemos añadir una columna "mobile" en este caso y luego decimos como va a ser en función de las otras normalmente

mobile_df = events_df.withColumn("mobile", col("device").isin("iOS", "Android"))
display(mobile_df)

# COMMAND ----------

# Para renombrar una columna

location_df = events_df.withColumnRenamed("geo", "location")
display(location_df)

# COMMAND ----------

# MAGIC %md ### Filtrar por condiciones a columnas

# COMMAND ----------

# ejemplo 1 

purchases_df = events_df.filter("ecommerce.total_item_quantity > 0")
display(purchases_df)

# COMMAND ----------

# Ejemplo 2 con .IsNotNull()

revenue_df = events_df.filter(col("ecommerce.purchase_revenue_in_usd").isNotNull())
display(revenue_df)

# COMMAND ----------

# Ejemplo 3 con dos condiciones y un operador and en este caso

android_df = events_df.filter((col("traffic_source") != "direct") & (col("device") == "Android"))
display(android_df)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Eliminar duplicados
# MAGIC 
# MAGIC dos formas

# COMMAND ----------

display(events_df.distinct())

# COMMAND ----------

distinct_users_df = events_df.dropDuplicates(["user_id"])


# COMMAND ----------

# También tenemos el método limit()

limit_df = events_df.limit(100)


# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Sort (ORder by)

# COMMAND ----------

# Ejemplo 1 

increase_timestamps_df = events_df.sort("event_timestamp")
display(increase_timestamps_df)

# COMMAND ----------

# Ejemplo 2

increase_timestamps_df = events_df.sort("event_timestamp")
display(increase_timestamps_df)

# COMMAND ----------

# Ejemplo 3

increase_sessions_df = events_df.orderBy(["user_first_touch_timestamp", "event_timestamp"])


# COMMAND ----------

DA.cleanup()


# COMMAND ----------


