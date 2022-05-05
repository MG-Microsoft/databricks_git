# contains essential logic to visualize covid-19 hospitalizations 
import wget
import pandas as pd
import pyspark.pandas as ps
import sys

# get latest data
def get_data(url, path):
    sys.stdout.fileno = lambda: False 
    wget.download(url, path)

# filter by country code
def filter_country(pdf, country='USA'):
    pdf = pdf[pdf.iso_code == country]
    return pdf

# pivot by indicator and fill missing values
def pivot_and_clean(pdf, fillna):
    pdf['value'] = pd.to_numeric(pdf['value'])
    pdf = pdf.fillna(fillna).pivot_table(values='value', columns='indicator', index='date')
    return pdf 

# create column names compatible with Delta Lake tables
def clean_spark_cols(pdf):
    clean_cols = pdf.columns.str.replace(' ', '_')
    pdf.columns = clean_cols
    psdf = ps.from_pandas(pdf)
    return psdf

# convert index to column (works with pandas API on Spark too)
def index_to_col(df, colname):
    df[colname] = df.index
    return df