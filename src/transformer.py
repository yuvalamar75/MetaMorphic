import pandas as pd

class Transformer:
    def __init__(self, logger):
        self.logger = logger

    def standardize_id_column(self, df: pd.DataFrame, columns: list) -> pd.DataFrame:
        for col in columns:
            if col not in df.columns:
                self.logger.error(f"Column '{col}' not found in DataFrame.")
                raise ValueError(f"Column '{col}' not found.")
            df[col] = df[col].astype(str).apply(self._standardize_id)
        return df

    def _standardize_id(self, id_str: str) -> str:
        # Remove dashes
        id_str = id_str.replace("-", "")
        # Convert to int to remove leading zeros, then back to string
        try:
            return str(int(id_str))
        except ValueError:
            self.logger.error(f"Non-numeric ID encountered: {id_str}")
            raise ValueError(f"Non-numeric ID encountered: {id_str}")
