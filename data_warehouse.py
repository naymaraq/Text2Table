from pyspark.sql import *
from pyspark.sql.types import *
import os
import shutil
import subprocess

spark = SparkSession.builder \
    .master("local") \
    .appName("Data Integration") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

def get_or_create_dataframe(schema, path=None, format="parquet"):
    if os.path.exists(path):
        df = spark.read.format("parquet").load(path, schema=schema)
    else:
        df = spark.createDataFrame([], schema)
    return df 

def append_new_row(df_name, schema, row):
    df = get_or_create_dataframe(schema, df_name)
    newRow = spark.createDataFrame(row)
    appended = df.union(newRow)
    return appended 

def save(df, df_name):

    tmp_df_name = "tmp_"+df_name
    df.write.save(tmp_df_name, format="parquet")

    if os.path.exists(df_name):
        subprocess.run(["rm", "-r", f"{df_name}"])

    if os.path.exists(tmp_df_name):
        subprocess.run(["mv", f"{tmp_df_name}", f"{df_name}"])

rel2schema = {
        "kill": {
                "schema": StructType([
                           StructField("killer", StringType(), True),
                           StructField("victim", StringType(), True)]), 
                "df_path": "kill.parquet"
                },
        "work_for": {
                "schema": StructType([
                           StructField("person", StringType(), True),
                           StructField("organization", StringType(), True)]),
                "df_path": "work_for.parquet"
                },
        "live_in": {
                "schema": StructType([
                           StructField("person", StringType(), True),
                           StructField("location", StringType(), True)]),
                "df_path": "live_in.parquet"
                },

        "located_in": {
                "schema": StructType([
                           StructField("location", StringType(), True),
                           StructField("location", StringType(), True)]),
                "df_path": "located_in.parquet"
                },
        "orgbased_in": {
                "schema": StructType([
                           StructField("organization", StringType(), True),
                           StructField("location", StringType(), True)]),
                "df_path": "orgbased_in.parquet"
                }
        }

def integrate(triples):
    for triple_list in triples:
        for rel, item1, item2 in triple_list:
            rel = rel.lower()
            schema = rel2schema[rel]["schema"]
            df_path = rel2schema[rel]["df_path"]
            appended = append_new_row(df_path, schema, [(item1, item2)])
            save(appended, df_path)

