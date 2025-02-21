import pandas as pd
from src.kpmg.pipeline.data_validation import DataValidation, DataIngestion
from typing import Union
import warnings
pd.options.display.float_format = "{:,.2f}".format
warnings.filterwarnings(action="ignore")


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
    null_df = DataValidation(df=DataIngestion(filepath="inputs/finance/credit_card_fraud.csv").read_data()).null_check()
    null_df = null_df[["ColumnName", "NullCount"]]
    return null_df

print(get_column_null_report())