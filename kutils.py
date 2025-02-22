import pandas as pd
import glob
from src.kpmg.pipeline.data_validation import DataValidation, DataIngestion
from typing import Union
import warnings
pd.options.display.float_format = "{:,.2f}".format
warnings.filterwarnings(action="ignore")
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, col
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

spark = SparkSession.builder.appName("kutils").getOrCreate()


def read_data() -> pd.DataFrame:
    df = pd.read_csv("artifacts/reports.csv")
    df.drop("Unnamed: 0", axis=1, inplace=True)
    df.set_index("Metrics", inplace=True)
    return df

def divide(new_df, df) -> Union[int, float]:
    try:
        value = (int(new_df.loc["missing_columns", 'Values']) / int(df.loc["total_Columns", "Values"]))*100
    except:
        value =0
    return value

def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    required_metrics = [
 'null_counts',
 'primarykey_duplicates',
 'duplicate_count',
 'missing_columns'
 ]
    new_df = df[df.index.isin(required_metrics)]
    new_df.loc["null_counts", "Pct"] =  (int(new_df.loc['null_counts', 'Values'])/int(df.loc["total_records", "Values"]))*100
    new_df.loc["primarykey_duplicates", "Pct"] =  (int(new_df.loc['primarykey_duplicates', 'Values'])/int(df.loc["total_records", "Values"]))*100
    new_df.loc["duplicate_count", "Pct"] =  (int(new_df.loc['duplicate_count', 'Values'])/int(df.loc["total_records", "Values"]))*100
    new_df.loc["missing_columns", 'Pct'] = divide(new_df, df)
    return new_df


def get_column_null_report() -> pd.DataFrame:
    todays = datetime.today().strftime("%Y%m%d")
    obj = DataValidation()
    obj.main()
    null_df = obj.null_check()
    null_df = null_df.select("ColumnName", "NullCount")
    null_report = null_df.toPandas()
    return null_report

def get_past_records():
    file_records = {}
    for file in glob.glob(pathname="archive/*.*"):
        filename = file.split("/")[1].split(".")[0]
        count = spark.read.csv(file).count()
        file_records[filename] = count
    
    schema = StructType(fields=
                            [
                            StructField("Date", StringType(), False),
                            StructField("Value", IntegerType(), False)
                            ]
                            )

    df = spark.createDataFrame([(k, v) for k, v in file_records.items()], schema)
    df = df.withColumn("Date", to_date(col("Date"), format="yyyyMMdd"))
    df = df.orderBy(col("Date").desc())
    pd_df = df.toPandas()
    pd_df.to_csv("artifacts/data_records.csv")
    return pd_df