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


killer_schema = StructType([
         StructField("killer", StringType(), True),
         StructField("victim", StringType(), True)])

killer_df_path = "kill.parquet"

appended = append_new_row(killer_df_path, killer_schema, [("Soghomon","Taliat")])
appended.show()
save(appended, killer_df_path)

