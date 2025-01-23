
import pandas as pd
import logging

class DataLoader:
    def __init__(self, logger: logging.Logger):
        self.logger = logger
        self.data = {}

    def load_data(self, file_path: str, file_type: str, sheet: str = None) -> pd.DataFrame:
        self.logger.info(f"Loading data from {file_path} as {file_type}")
        try:
            if file_type.lower() == 'csv':
                df = pd.read_csv(file_path)
            elif file_type.lower() in ['xlsx', 'excel']:
                df = pd.read_excel(file_path, sheet_name=sheet,engine='openpyxl')
            else:
                raise ValueError(f"Unsupported file type: {file_type}")
            self.logger.info(f"Successfully loaded data from {file_path}")
            return df
        except Exception as e:
            self.logger.error(f"Error loading data from {file_path}: {e}", exc_info=True)
            raise  # Re-raise the exception after logging