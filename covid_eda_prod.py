# Databricks notebook source
# MAGIC %md
# MAGIC ### View the latest COVID-19 hospitalization data

# COMMAND ----------

# MAGIC %md
# MAGIC #### Setup 

# COMMAND ----------

#test011

# COMMAND ----------

# MAGIC %pip install -r requirements.txt

# COMMAND ----------

# MAGIC %load_ext autoreload
# MAGIC %autoreload 2

# COMMAND ----------

dbutils.widgets.dropdown(name="run as", choices=["testing", "production"], defaultValue="testing")

run_as = dbutils.widgets.get("run as")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Get and Transform data

# COMMAND ----------

from covid_analysis.transforms import *

url = "https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/hospitalizations/covid-hospitalizations.csv"
path = "/tmp/covid-hospitalizations.csv"

get_data(url, path)

# COMMAND ----------

import pandas as pd

# read from /tmp, subset for USA, pivot and fill missing values
df = pd.read_csv("/tmp/covid-hospitalizations.csv")
df = filter_country(df, country='USA')
df = pivot_and_clean(df, fillna=0)  
df = clean_spark_cols(df)
df = index_to_col(df, colname='date')

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Save to Delta Lake
# MAGIC The current schema has spaces in the column names, which are incompatible with Delta Lake.  To save our data as a table, we'll replace the spaces with underscores.  We also need to add the date index as its own column or it won't be available to others who might query this table.

# COMMAND ----------

# Write to Delta Lake
df.to_table(name=run_as+"_covid_analysis", mode='overwrite')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Visualize

# COMMAND ----------

# Using Databricks visualizations and data profiling
display(spark.table(run_as+"_covid_analysis"))

# COMMAND ----------

# Using python
df.to_pandas().plot(figsize=(13,6), grid=True).legend(loc='upper left')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Clean up testing table 

# COMMAND ----------

if run_as == "testing":
  spark.sql("DROP TABLE testing_covid_analysis")
