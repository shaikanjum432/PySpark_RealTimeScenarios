# Databricks notebook source
# MAGIC %md
# MAGIC Data Partitioning is critical to data processing performance especially for large volume of data processing in Spark.

# COMMAND ----------

df_emp_csv = spark.read.option("nullValue","null").csv("/FileStore/tables/emp.csv", header = True, inferSchema = True)
display(df_emp_csv)

# COMMAND ----------

#Creating new columns YEAR and MONTH based on hiredate date field

from pyspark.sql.functions import date_format
df_emp_csv = df_emp_csv.withColumn("YEAR",date_format("HIREDATE",'yyyy')).withColumn("MONTH",date_format("HIREDATE",'MM'))
df_emp_csv.show()

# COMMAND ----------

# partitioning YEAR and MONTH columns using PatitonBY
df_emp_csv.write.format("delta").partitionBy("YEAR","MONTH").mode("overwrite").saveAsTable("emp_part")

# COMMAND ----------

# MAGIC %fs ls /user/hive/warehouse/emp_part/YEAR=1981

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM emp_part WHERE YEAR = '1980'

# COMMAND ----------

# MAGIC %sql
# MAGIC EXPLAIN SELECT * FROM emp_part WHERE YEAR = '1980'

# COMMAND ----------


