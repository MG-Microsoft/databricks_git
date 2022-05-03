# Databricks notebook source
# MAGIC %md
# MAGIC #### Get the latest COVID-19 hospitalization data

# COMMAND ----------

# MAGIC %pip install -r ./requirements.txt

# COMMAND ----------

# MAGIC %load_ext autoreload
# MAGIC %autoreload 2

# COMMAND ----------

from src.utils.transforms import *

url = "https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/hospitalizations/covid-hospitalizations.csv"
path = "/tmp/covid-hospitalizations.csv"

get_data(url, path)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Transform data

# COMMAND ----------

import pandas as pd
from src.utils.transforms import *

# read from /tmp, subset for USA, pivot and fill missing values
df = pd.read_csv("/tmp/covid-hospitalizations.csv")
df = filter_country(df, country='USA')
df = pivot_and_clean(df, fillna=0)  
df = clean_spark_cols(df)
df = index_to_col(df, colname='date')

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Visualize 

# COMMAND ----------

df.plot(figsize=(13,6), grid=True).legend(loc='upper left')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Save to Delta Lake
# MAGIC The current schema has spaces in the column names, which are incompatible with Delta Lake.  To save our data as a table, we'll replace the spaces with underscores.  We also need to add the date index as its own column or it won't be available to others who might query this table.

# COMMAND ----------

import pyspark.pandas as ps
# Write to Delta table, overwrite with latest data each time
psdf = ps.from_pandas(df)
psdf.to_table(name='covid_usa', mode='overwrite')

# COMMAND ----------

# MAGIC %md
# MAGIC #### View table

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM covid_usa

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY covid_usa
