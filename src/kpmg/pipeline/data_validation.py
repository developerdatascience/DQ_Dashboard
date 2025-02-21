from typing import Dict, Optional, Union
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, count, when, lit, round, sum
from pyspark.sql.types import IntegerType, StringType, BooleanType, FloatType, DataType, StructField, StructType
import logging
from src.kpmg.utils import client
from src.kpmg.pipeline.data_ingestion import DataIngestion
from pathlib import Path
import pandas as pd


logger = logging.getLogger(__name__)

class DataValidation:
    def __init__(self, df):
        self.df = df
        self.client = client.Client()
        self.null_report: Dict[str, Union[int, float, None]] = {}
        self.spark = SparkSession.builder.appName("datavalidation").getOrCreate()

    
    def null_check(self) -> DataFrame:
        """Returns a dataframe with null count"""
        logger.info("Scanning for Null values 🚀")
        for column in self.df.columns:
            null_count = self.df.select(sum(col(column).isNull().cast('int'))).collect()[0][0]
            self.null_report[column] = null_count
        
        schema = StructType(fields=
                            [
                            StructField("ColumnName", StringType(), False),
                            StructField("NullCount", IntegerType(), False)
                            ]
                            )
        null_df = self.spark.createDataFrame([(k, v) for k, v in self.null_report.items()], schema)
        null_df = null_df.withColumn("TotalRows", lit(self.df.count())) \
                        .withColumn("NullPct", round((col("NullCount")/col("TotalRows"))*100,0)) \
                        .withColumn("Valid", when(col("NullPct") > 0.0, lit("❌")).otherwise(lit("✅")))
        report_df = null_df.toPandas()
        logger.info("Null check report completed. ✅")
        return report_df

    def primaryKeyCheck(self):
        logger.info("Checking for presence Primary key Rule")
        primary_key: str = self.client.getConfig("subjects.credit_card.primary_key")
        report_dict = {}
        datatype = self.client.getConfig("subjects.credit_card.DataType.TransactionID")
        if primary_key in self.df.columns:
            logger.info(f"Primary Key: {primary_key} ✅")
            report_dict[primary_key] = "True"
        else:
            logger.info(f"Primary Key: {primary_key} is missing ❌")
            report_dict[primary_key] = "False"
        
        if self.df.schema[primary_key].dataType == datatype:
            logger.debug(f"DataType for {primary_key} does not match ❌")
        else:
            logger.info(f"{primary_key} datatype matched successfully ✅")
        schema = StructType([
        StructField("PrimaryKey", StringType(), False),
        StructField("Check", StringType(), False)])
        report_df = self.spark.createDataFrame([(k, v) for k, v in report_dict.items()], schema)
        report_df = report_df.withColumn("Valid", when(col("Check") == "True", lit("✅")).otherwise(lit("❌")))
        logger.info("Checking for presence Primary key Rule completed")
        return report_df
    
    def primaryKeyDuplicateCheck(self):
        logger.info("Running primaryKeyDuplicateCheck Rule 🏃‍♀️")
        primary_key: str = self.client.getConfig("subjects.credit_card.primary_key")
        dup_df = self.df.groupBy(primary_key).count().alias("count").filter("count > 1")
        dup_df = dup_df.withColumn("Valid", lit("❌"))
        primary_key_dup = [row[0] for row in dup_df.select(col(primary_key)).collect()]
        if dup_df.count() > 1:
            logger.info(f"Duplicate Value found ❌: {primary_key_dup}") 
        else:
            logger.info("No Duplicate value found ✅")
        logger.info("primaryKeyDuplicateCheck Rule Completed ✅")
        return dup_df
            

    def duplicateValuesCheck(self) -> DataFrame:
        """Returns a DataFrame of duplicate rows and their counts."""
        logger.info("Checking for presence of duplicate values 🏃‍♀️")
        duplicate_df = self.df.groupBy(self.df.columns).count().alias("count").filter("count > 1")
        duplicate_df = duplicate_df.withColumn("Check", lit("❌"))
        logger.info("Checking for presence of duplicate values completed ✅")
        return duplicate_df
    
    def missing_column_check(self) -> DataFrame:
        """Returns a dataframe with missing columns"""
        logger.info("Scanning for required columns")
        columns = self.client.getConfig("subjects.credit_card.column_sequence")
        schema = StructType(fields=
                            [
                            StructField("ColumnName", StringType(), False),
                            StructField("ColumnPresentCheck", StringType(), False)
                            ]
                            )
        data_dict = {}
        for column in columns:
            if column in self.df.columns:
                data_dict[column] = 'True'
            else:
                data_dict[column] = 'False'
        
        final_df = self.spark.createDataFrame([(k, v) for k, v in data_dict.items()], schema)
        final_df = final_df.withColumn("IsPresent", when(col("ColumnPresentCheck") == "True", lit("✅")).otherwise(lit("❌")))
        return final_df
                

    def rangeCheck(self) -> DataFrame:
        """Return a Dataframe with rows having value out of range """
        logger.info("Scanning values for out of range 🚀")
        min = self.client.getConfig("subjects.credit_card.Constraints.TransactionAmount.min")
        max = self.client.getConfig("subjects.credit_card.Constraints.TransactionAmount.max")

        rejected_df = self.df.withColumn("TransactionAmtRange", 
                                     when((col("TransactionAmount") < min) | (col("TransactionAmount") > max), "OutOfRange") \
                                     .otherwise("Valid")) \
                                     .filter("TransactionAmtRange = OutOfRange")
        logger.info("Scanning values for out of range Completed ✅")
        
        return rejected_df
        
        
    
    def datatimeCheck(self):
        pass


    def main(self):
        logger.info("Running Checks.....🔥🏁")
        schema = StructType([
            StructField("Metrics", StringType(), False),
            StructField("Values", StringType(), False)
        ])
        stats = stats = {
            'total_records': self.df.count(),
            'total_Columns': len(self.df.columns),
            'null_counts': "",
            'primary_key_check': "",
            'primarykey_duplicates': "",
            'duplicate_count': 0,
            'missing_columns': "",
            # 'out_of_range': {},
            # 'invalid_dates': {}
        }
        stats['null_counts'] = self.null_check().select(sum(col("NullCount"))).collect()[0][0]
        stats['primary_key_check'] = self.primaryKeyCheck().select("Check").collect()[0][0]
        stats["primarykey_duplicates"] = self.primaryKeyDuplicateCheck().count()
        stats["duplicate_count"] = self.duplicateValuesCheck().count()
        stats["missing_columns"] = self.missing_column_check().filter("ColumnPresentCheck=False").count()

        reports = self.spark.createDataFrame([(k, v) for k, v in stats.items()], schema)
        reports = reports.toPandas()
        reports.head()
        reports.to_csv("artifacts/reports.csv", index=True)

        logger.info("Check Completed......🛁")



        return reports

    

if __name__ == "__main__":
    obj= DataValidation(
        df=DataIngestion(filepath="inputs/finance/credit_card_fraud.csv").read_data()
    )
    print(obj.null_check().head())
