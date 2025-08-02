from networksecurity.entity.config_entity import DataValidationConfig
from networksecurity.entity.artifact_entity import DataIngestionArtifact, DataValidationArtifact
from networksecurity.exception.exception import NetworkSecurityException
from networksecurity.logging.logger import logging
from networksecurity.utils.main_utils.utils import read_yaml_file, write_yaml_file
from networksecurity.constants.training_pipeline import SCHEMA_FILE_PATH
from scipy.stats import ks_2samp
import os, sys
import pandas as pd
from typing import List

class DataValidation:
    def __init__(self,data_ingestion_artifact: DataIngestionArtifact,
                 data_validation_config: DataValidationConfig):
        try:
            self.data_ingestion_artifact = data_ingestion_artifact
            self.data_validation_config = data_validation_config
            self._schema_config = read_yaml_file(SCHEMA_FILE_PATH)
        except Exception as e:
            raise NetworkSecurityException(e, sys)
        
    @staticmethod
    def read_data(file_path: str) -> pd.DataFrame:
        """
        Reads a CSV file and returns a pandas DataFrame.
        :param file_path: Path to the CSV file.
        :return: DataFrame containing the data.
        """
        try:
            return pd.read_csv(file_path)
        except Exception as e:
            raise NetworkSecurityException(e, sys)
        
    def validate_number_of_columns(self,dataframe: pd.DataFrame) -> bool:
        try:
            no_of_columns=len(self._schema_config)
            logging.info(f"required number of columns: {no_of_columns}")
            logging.info(f"Data frame has {len(dataframe.columns)} columns.")
            if len(dataframe.columns) == no_of_columns:
                logging.info("Number of columns validation passed.")
                return True
            else:
                logging.error(f"Number of columns validation failed. Expected: {no_of_columns}, Found: {len(dataframe.columns)}")
                return False
        except Exception as e:
            raise NetworkSecurityException(e, sys)
    
    def validate_numerical_columns_exist(self, dataframe: pd.DataFrame) -> bool:
        try:
            required_numerical_columns: List[str] = self._schema_config.get("numerical_columns", [])
            missing_columns = [col for col in required_numerical_columns if col not in dataframe.columns]
            
            if missing_columns:
                logging.error(f"Missing numerical columns: {missing_columns}")
                return False
            
            logging.info("All required numerical columns exist.")
            return True
        
        except Exception as e:
            raise NetworkSecurityException(e, sys)

    def detect_dataset_drift(self,base_df,current_df,threshold=0.05)-> bool:
        try:
            status= True
            report = {}
            for column in base_df.columns:
                d1=base_df[column]
                d2=current_df[column]
                is_sample_dist = ks_2samp(d1, d2)
                if threshold <= is_sample_dist.pvalue:
                    is_found = False
                else:
                    is_found = True
                    status= False
                report.update({column: {
                    "p_value": is_sample_dist.pvalue,
                    "drift_status": is_found
                }})
            drift_report_file_path = self.data_validation_config.drift_report_file_path
            dir_path = os.path.dirname(drift_report_file_path)
            os.makedirs(dir_path, exist_ok=True)
            write_yaml_file(drift_report_file_path, report, replace=True)
            logging.info(f"Drift report saved at {drift_report_file_path}")
        except Exception as e:
            raise NetworkSecurityException(e, sys)
        
    def initiate_data_validation(self) -> DataValidationArtifact:
        try:
            train_file_path = self.data_ingestion_artifact.trained_file_path
            test_file_path = self.data_ingestion_artifact.test_file_path
            ## Read the data
            train_dataframe = DataValidation.read_data(train_file_path)
            test_dataframe = DataValidation.read_data(test_file_path)
            logging.info("Data read successfully from train and test files.")
            ##validate no of columns
            status = self.validate_number_of_columns(train_dataframe)
            if not status:
                error_message = f"Number of columns in train file {train_file_path} is not as per schema."
                logging.error(error_message)
            status = self.validate_number_of_columns(test_dataframe)
            if not status:
                error_message = f"Number of columns in test file {test_file_path} is not as per schema."
                logging.error(error_message)
            ##validate numerical columns
            status = self.validate_numerical_columns_exist(train_dataframe)
            if not status:
                error_message = f"Numerical columns in train file {train_file_path} are not as per schema."
                logging.error(error_message)
            status = self.validate_numerical_columns_exist(test_dataframe)
            if not status:
                error_message = f"Numerical columns in test file {test_file_path} are not as per schema."
                logging.error(error_message)
            ## Check for data drift
            status = self.detect_dataset_drift(base_df=train_dataframe, current_df=test_dataframe)
            dir_path = os.path.dirname(self.data_validation_config.valid_train_file_path)
            os.makedirs(dir_path, exist_ok=True)
            train_dataframe.to_csv(self.data_validation_config.valid_train_file_path, index=False, header=True)
            test_dataframe.to_csv(self.data_validation_config.valid_test_file_path, index=False, header=True)
            
            data_validation_artifact = DataValidationArtifact(
                validation_status=status,
                valid_train_file_path=self.data_ingestion_artifact.trained_file_path,
                valid_test_file_path=self.data_ingestion_artifact.test_file_path,
                invalid_train_file_path=None,
                invalid_test_file_path=None,
                drift_report_file_path=self.data_validation_config.drift_report_file_path,
            )
            return data_validation_artifact
        except Exception as e:
            raise NetworkSecurityException(e, sys)