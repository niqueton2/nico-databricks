# Databricks notebook source
# MAGIC %run ../Includes/Classroom-Setup-00.07

# COMMAND ----------

# MAGIC %md ## DataFrameReader
# MAGIC Interface used to load a DataFrame from external storage systems
# MAGIC 
# MAGIC **`spark.read.parquet("path/to/files")`**
# MAGIC 
# MAGIC DataFrameReader is accessible through the SparkSession attribute **`read`**. This class includes methods to load DataFrames from different external storage systems.

# COMMAND ----------

# MAGIC %md ### Read from CSV files
# MAGIC Read from CSV with the DataFrameReader's **`csv`** method and the following options:
# MAGIC 
# MAGIC Tab separator, use first line as header, infer schema

# COMMAND ----------

users_df = (spark
           .read
           .option("sep", "\t")
           .option("header", True)
           .option("inferSchema", True)
           .csv(DA.paths.users_csv)
          )

users_df.printSchema()

# COMMAND ----------

display(users_df)

# COMMAND ----------

# También se pueden especificar los parámetros así

users_df = (spark
           .read
           .csv(DA.paths.users_csv, sep="\t", header=True, inferSchema=True)
          )

users_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Podemos escribir manualmente el esquema y luego pasarlo como parámetro en vez de inferirlo
# MAGIC Esto no lo vemos porque es un coñazo escribir la estructura así
# MAGIC 
# MAGIC También podemos usar algo más del tipo DDL

# COMMAND ----------

ddl_schema = "user_id string, user_first_touch_timestamp long, email string"

users_df = (spark
           .read
           .option("sep", "\t")
           .option("header", True)
           .schema(ddl_schema)
           .csv(DA.paths.users_csv)
          )

display(users_df)

# COMMAND ----------

# MAGIC %md ### Read from JSON files
# MAGIC 
# MAGIC Read from JSON with DataFrameReader's **`json`** method and the infer schema option

# COMMAND ----------

events_df = (spark
              .read
            .option("inferSchema", True)
            .json(DA.paths.events_json)
           )

events_df.printSchema()

# COMMAND ----------

display(events_df)

# COMMAND ----------

# Lo podemos hacer también indicandole la estructura, que patea. Por lo visto así lee más rápido

from pyspark.sql.types import ArrayType, DoubleType, IntegerType, LongType, StringType, StructType, StructField

user_defined_schema = StructType([
    StructField("device", StringType(), True),
    StructField("ecommerce", StructType([
        StructField("purchaseRevenue", DoubleType(), True),
        StructField("total_item_quantity", LongType(), True),
        StructField("unique_items", LongType(), True)
    ]), True),
    StructField("event_name", StringType(), True),
    StructField("event_previous_timestamp", LongType(), True),
    StructField("event_timestamp", LongType(), True),
    StructField("geo", StructType([
        StructField("city", StringType(), True),
        StructField("state", StringType(), True)
    ]), True),
    StructField("items", ArrayType(
        StructType([
            StructField("coupon", StringType(), True),
            StructField("item_id", StringType(), True),
            StructField("item_name", StringType(), True),
            StructField("item_revenue_in_usd", DoubleType(), True),
            StructField("price_in_usd", DoubleType(), True),
            StructField("quantity", LongType(), True)
        ])
    ), True),
    StructField("traffic_source", StringType(), True),
    StructField("user_first_touch_timestamp", LongType(), True),
    StructField("user_id", StringType(), True)
])

events_df = (spark
            .read
            .schema(user_defined_schema)
            .json(DA.paths.events_json)
           )

# COMMAND ----------

# MAGIC %md inferir un esquema por lo visto no es lo suyo, puede liarla mucho si la tabla es grande. Nos queda la opción de arriba o escribir el ddl como en el caso del csv

# COMMAND ----------

# Vamos a pasar ahora un df a un archivo parquet

users_output_dir = DA.paths.working_dir + "/users.parquet"

(users_df
 .write
 .option("compression", "snappy")
 .mode("overwrite")
 .parquet(users_output_dir)
)

# COMMAND ----------

display(
    dbutils.fs.ls(users_output_dir)
)

# COMMAND ----------

# MAGIC %md ### Write DataFrames to tables
# MAGIC 
# MAGIC Write **`events_df`** to a table using the DataFrameWriter method **`saveAsTable`**
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/icon_note_32.png" alt="Note"> This creates a global table, unlike the local view created by the DataFrame method **`createOrReplaceTempView`**

# COMMAND ----------

events_df.write.mode("overwrite").saveAsTable("events")

# COMMAND ----------

print(DA.schema_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write Results to a Delta Table
# MAGIC 
# MAGIC Write **`events_df`** with the DataFrameWriter's **`save`** method and the following configurations: Delta format & overwrite mode.

# COMMAND ----------

# Es importante guardar las tablas en formato delta porque así es como mejor las entiende y usa databricks

events_output_path = DA.paths.working_dir + "/delta/events"

(events_df
 .write
 .format("delta")
 .mode("overwrite")
 .save(events_output_path)
)

# COMMAND ----------

DA.cleanup()

# COMMAND ----------


