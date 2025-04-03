import pandas as pd
import logging
import os

class DataLoader:
    def __init__(self, logger: logging.Logger):
        self.logger = logger.getChild(self.__class__.__name__)
        self.data = {}

    def load_data(self, file_path: str, file_type: str, sheet: str = None) -> pd.DataFrame:
        self.logger.info(f"Loading data from {file_path} sheet name : {sheet} as {file_type} ")
        
        # Check if file exists
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")
            
        try:
            if file_type.lower() == 'csv':
                df = pd.read_csv(file_path)
            elif file_type.lower() in ['xlsx', 'excel']:
                # Try with xlrd engine first
                try:
                    df = pd.read_excel(
                        file_path,
                        sheet_name=sheet,
                        engine='openpyxl'
                    )
                except Exception as e1:
                    self.logger.warning(f"Failed to read : {e1}")
                    # Try with openpyxl as backup

            
            else:
                raise ValueError(f"Unsupported file type: {file_type}")
                
            self.logger.info(f"Successfully loaded data from {file_path} sheet name : {sheet}")
            return df
        except Exception as e:
            self.logger.error(f"Error loading data from {file_path} sheet name : {sheet} : {e}", exc_info=True)
            raise  # Re-raise the exception after logging