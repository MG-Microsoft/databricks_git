# Databricks notebook source
# MAGIC %md
# MAGIC #### Get latest COVID-19 hospitalization data

# COMMAND ----------

!wget -q https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/hospitalizations/covid-hospitalizations.csv -O /tmp/covid-hospitalizations.csv

# COMMAND ----------

# MAGIC %md #### Read

# COMMAND ----------

import pandas as pd

# read from /tmp, subset for USA, pivot and fill missing values
df = pd.read_csv("/tmp/covid-hospitalizations.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Visualize 

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Data Transformations

# COMMAND ----------

# MAGIC %pip install bamboolib --quiet

# COMMAND ----------

import bamboolib as bam
bam

# COMMAND ----------

import plotly.express as px
fig = px.line(df.sort_values(by=['date'], ascending=[True]), x='date', y=['Daily_ICU_occupancy', 'Daily_ICU_occupancy_per_million', 'Daily_hospital_occupancy', 'Daily_hospital_occupancy_per_million', 'Weekly_new_hospital_admissions', 'Weekly_new_hospital_admissions_per_million'], title='Covid-19 Trends 2020-present', template='plotly_white')
fig

# COMMAND ----------

# MAGIC  %md
# MAGIC #### Save to Delta Lake

# COMMAND ----------

import pandas as pd
df = pd.read_csv(r'/tmp/covid-hospitalizations.csv', sep=',', decimal='.')
# Step: Change data type of date to Datetime
df['date'] = pd.to_datetime(df['date'], infer_datetime_format=True)

# Step: Keep rows where iso_code is one of: USA
df = df.loc[df['iso_code'].isin(['USA'])]

# Step: Manipulate strings of 'indicator' via Find ' ' and Replace with '_'
df["indicator"] = df["indicator"].str.replace(' ', '_', regex=False)

# Step: Pivot dataframe from long to wide format using the variable column 'indicator' and the value column 'value'
df = df.set_index(['entity', 'iso_code', 'date', 'indicator'])['value'].unstack(-1).reset_index()
df.columns.name = ''

# Step: Replace missing values
df = df.fillna(0)

# Step: Databricks: Write to database table
spark.createDataFrame(df).write.mode("overwrite").option("mergeSchema", "true").saveAsTable("covid_trends")

# COMMAND ----------

# MAGIC %md
# MAGIC #### View table

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM covid_trends
