# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ### Primera sesión (pyspark)

# COMMAND ----------

# MAGIC %run ../nico-databricks/Includes/Classroom-Setup-00.01

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT name, price
# MAGIC FROM products
# MAGIC WHERE price < 200
# MAGIC ORDER BY price

# COMMAND ----------

# Podemos hacer la query con pyspark también y display se ve como una tabla

display(spark
        .table("products")
        .select("name", "price")
        .where("price < 200")
        .orderBy("price")
       )

# COMMAND ----------

budget_df = (spark
             .table("products")
             .select("name", "price")
             .where("price < 200")
             .orderBy("price")
            )

# COMMAND ----------

display(budget_df)

# COMMAND ----------

budget_df.schema

# COMMAND ----------

budget_df.printSchema()

# COMMAND ----------

products_df=spark.sql("select * from products")

# COMMAND ----------

(products_df
  .select("name", "price")
  .where("price < 200")
  .orderBy("price")
  .show())

# COMMAND ----------

budget_df.count()

# COMMAND ----------

budget_df.collect()

# COMMAND ----------

# MAGIC %md ### Creando vistas temporales a partir de dataframes

# COMMAND ----------

budget_df.createOrReplaceTempView("budget")

# COMMAND ----------

display(spark.sql("SELECT * FROM budget"))

# COMMAND ----------

DA.cleanup()

# COMMAND ----------


