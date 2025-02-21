from pathlib import Path
from pyspark.sql import SparkSession, DataFrame
from src.kpmg.utils import client
from datetime import datetime
import logging
logger = logging.getLogger(__name__)


class DataIngestion:
    def __init__(self):
        self.filepath = f"inputs/finance/{datetime.today().strftime("%Y%m%d")}.csv"
        self.spark = SparkSession.builder.appName("DataIngestion").getOrCreate()

        self.client = client.Client()

        if not Path(self.filepath).exists():
            logger.info(f"{self.filepath} does not exists")
        
        if self.filepath.split(".")[1] == "csv":
            logger.debug(f"Only csv file is accepted: {self.filepath}")
        

    
    def read_data(self) -> DataFrame:
        logger.info(f"Reading data from {self.filepath}")
        try:
            df = self.spark.read.csv(self.filepath, header=True, inferSchema=True)
            logger.info("File imported successfully")
            return df
        except Exception as e:
            logger.debug(f"Error while reading the file: {e}")


if __name__ == "__main__":
    df = DataIngestion().read_data()
    print(df.show())