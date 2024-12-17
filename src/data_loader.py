import pandas as pd

class DataLoader:
    def __init__(self, logger):
        self.logger = logger
        self.data = {}

    def load_data(self, file_path: str, file_type: str, sheet: str = None) -> pd.DataFrame:
        self.logger.info(f"Loading data from {file_path}")
        if file_type == 'csv':
            df = pd.read_csv(file_path)
            
        elif file_type == 'xlsx':
            df = pd.read_excel(file_path, sheet_name=sheet)
        else:
            raise ValueError(f"Unsupported file type: {file_type}")
        return df
