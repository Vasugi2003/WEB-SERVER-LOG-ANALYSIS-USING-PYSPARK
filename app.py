import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract, concat, lit

# Set up the Spark session
spark = SparkSession.builder.appName("LogFileAnalysis").getOrCreate()

# File path (you can modify this if you upload files or use other paths)
logs_file_path = "D:/BDA PROJECT/webserver_log_analysis/app.py"

# Read the logs into a Spark DataFrame
base_df = spark.read.text(logs_file_path)

# Extract relevant fields using regex
split_df = base_df.select(
    regexp_extract('value', r'^([^\s]+\s)', 1).alias('host'),
    regexp_extract('value', r'^.*\[(\d\d\/\w{3}\/\d{4}:\d{2}:\d{2}:\d{2} -\d{4})]', 1).alias('timestamp'),
    regexp_extract('value', r'^.*"\w+\s+([^\s]+)\s+HTTP.*"', 1).alias('path'),
    regexp_extract('value', r'^.*"\s+([^\s]+)', 1).cast('integer').alias('status'),
    regexp_extract('value', r'^.*\s+(\d+)$', 1).cast('integer').alias('content_size')
)

# Clean data
cleaned_df = split_df.na.fill({'content_size': 0})

# Title and description
st.title('Web Server Log Analysis')
st.write("Analyze web server logs using PySpark and visualize results.")

# Analysis 1: Hosts with most requests
st.subheader('Top Hosts by Request Count')
df_host = cleaned_df.groupBy('host').count().orderBy('count', ascending=False).limit(10)
df_host_pandas = df_host.toPandas()

# Barplot for hosts
st.write(df_host_pandas)
fig, ax = plt.subplots()
sns.barplot(x='host', y='count', data=df_host_pandas, ax=ax)
ax.set_xticklabels(ax.get_xticklabels(), rotation=90)
st.pyplot(fig)

# Analysis 2: Most frequent HTTP paths
st.subheader('Top HTTP Paths')
df_path = cleaned_df.groupBy('path').count().orderBy('count', ascending=False).limit(10)
df_path_pandas = df_path.toPandas()

# Barplot for HTTP paths
st.write(df_path_pandas)
fig, ax = plt.subplots()
sns.barplot(x='path', y='count', data=df_path_pandas, ax=ax)
ax.set_xticklabels(ax.get_xticklabels(), rotation=90)
st.pyplot(fig)

# Analysis 3: HTTP status codes distribution
st.subheader('HTTP Status Codes')
status_count = cleaned_df.groupBy('status').count().orderBy('count', ascending=False)
status_count_pandas = status_count.toPandas()

# Barplot for status codes
st.write(status_count_pandas)
fig, ax = plt.subplots()
sns.barplot(x='status', y='count', data=status_count_pandas, ax=ax)
st.pyplot(fig)

# Analysis 4: Content size distribution
st.subheader('Content Size Distribution')
size_counts = cleaned_df.groupBy('content_size').count().orderBy('count', ascending=False).limit(10)
size_counts_pandas = size_counts.toPandas()

# Barplot for content size
st.write(size_counts_pandas)
fig, ax = plt.subplots()
sns.barplot(x='content_size', y='count', data=size_counts_pandas, ax=ax)
st.pyplot(fig)

