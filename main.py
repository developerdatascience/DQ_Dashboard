from src.kpmg import logger
from src.kpmg.pipeline.data_ingestion import DataIngestion
from src.kpmg.pipeline.data_validation import DataValidation


STAGE_NAME = "DATA INGESTION STAGE"

try:
    logger.info(f">>>>>>>>>>>> stage {STAGE_NAME} started <<<<<<<<<<<<")
    obj= DataIngestion(filepath="inputs/finance/credit_card_fraud.csv")
    obj.read_data()
    logger.info(f">>>>>>>>>>>> stage {STAGE_NAME} completed <<<<<<<<<<<<\n\n")
except Exception as e:
    logger.exception(e)
    raise e


STAGE_NAME = "DATA VALIDATION STAGE"

try:
    logger.info(f">>>>>>>>>>>> stage {STAGE_NAME} started <<<<<<<<<<<<")
    obj= DataValidation(
        df=DataIngestion(filepath="inputs/finance/credit_card_fraud.csv").read_data()
    )
    obj.main()
    logger.info(f">>>>>>>>>>>> stage {STAGE_NAME} completed <<<<<<<<<<<<\n\n")
except Exception as e:
    logger.exception(e)
    raise e

