# Databricks notebook source
# MAGIC %run ../Includes/Classroom-Setup-00.08L

# COMMAND ----------

# MAGIC %md ### 1. Read with infer schema
# MAGIC - View the first CSV file using DBUtils method **`fs.head`** with the filepath provided in the variable **`single_product_csv_file_path`**
# MAGIC - Create **`products_df`** by reading from CSV files located in the filepath provided in the variable **`products_csv_path`**
# MAGIC   - Configure options to use first line as header and infer schema

# COMMAND ----------

single_product_csv_file_path = f"{DA.paths.products_csv}/part-00000-tid-1663954264736839188-daf30e86-5967-4173-b9ae-d1481d3506db-2367-1-c000.csv"
print(dbutils.fs.head(single_product_csv_file_path))

products_csv_path = DA.paths.products_csv
products_df = (spark.read
               .option("header",True)
              .csv(products_csv_path))

products_df.printSchema()

# COMMAND ----------

display(products_df)

# COMMAND ----------

assert(products_df.count() == 12)
print("All test pass")

# COMMAND ----------

# MAGIC %md ### 2. Read with user-defined schema
# MAGIC Define schema by creating a **`StructType`** with column names and data types

# COMMAND ----------

user_defined_schema_ddl = "item_id string,name string,price double"

products_df2 =(spark.read
              .option("header",True)
               .schema(user_defined_schema_ddl)
              .csv(products_csv_path)
              )

# COMMAND ----------

display(products_df2)

# COMMAND ----------

# MAGIC %md ### 4. Write to Delta
# MAGIC Write **`products_df`** to the filepath provided in the variable **`products_output_path`**

# COMMAND ----------

products_output_path = DA.paths.working_dir + "/delta/products"
(products_df
 .write
 .format("delta")
 .mode("overwrite")
 .save(products_output_path)
)

# COMMAND ----------

verify_files = dbutils.fs.ls(products_output_path)
verify_delta_format = False
verify_num_data_files = 0
for f in verify_files:
    if f.name == "_delta_log/":
        verify_delta_format = True
    elif f.name.endswith(".parquet"):
        verify_num_data_files += 1

assert verify_delta_format, "Data not written in Delta format"
assert verify_num_data_files > 0, "No data written"
del verify_files, verify_delta_format, verify_num_data_files
print("All test pass")

# COMMAND ----------

DA.cleanup()

# COMMAND ----------


