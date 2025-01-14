import pandas as pd
import unicodedata

class Transformer:
    def __init__(self, logger,df):
        self.logger = logger
        self.df = df

    def standardize_id_columns(self,  columns: list) -> pd.DataFrame:
        for col in columns:
            if col not in self.df.columns:
                self.logger.error(f"Column '{col}' not found in DataFrame.")
                raise ValueError(f"Column '{col}' not found.")
            self.df[col] = self.df[col].astype(str).apply(self._standardize_id)
    

    def _standardize_id(self, id_str: str) -> str:
        # Remove dashes
        id_str = id_str.replace("-", "")
        # Convert to int to remove leading zeros, then back to string
        try:
            return str(int(id_str))
        except ValueError:
            self.logger.error(f"Non-numeric ID encountered: {id_str}")
            raise ValueError(f"Non-numeric ID encountered: {id_str}")
    
    def normalize_string(self, s):
        """Normalize a string to NFC form."""
        if isinstance(s, str):
            return unicodedata.normalize('NFC', s)
        return s

    def run_transformations(self, config: dict) -> pd.DataFrame:
            self.logger.info("Starting transformations...")

            for idx, transformation in enumerate(config, start=1):
                if not isinstance(transformation, dict):
                    self.logger.error(f"Invalid transformation format at index {idx}: {transformation}")
                    raise ValueError(f"Each transformation should be a dictionary. Error at index {idx}.")

                for method_name, params in transformation.items():
                    # Normalize method name and parameters
                    method_name_normalized = self.normalize_string(method_name)
                    if isinstance(params, list):
                        params_normalized = [self.normalize_string(param) for param in params]
                    elif isinstance(params, dict):
                        params_normalized = {k: self.normalize_string(v) for k, v in params.items()}
                    else:
                        params_normalized = self.normalize_string(params)

                    self.logger.info(f"Applying transformation '{method_name_normalized}' with parameters {params_normalized}")
                    method = getattr(self, method_name_normalized, None)

                    if not method:
                        self.logger.error(f"Transformation method '{method_name_normalized}' not found.")
                        raise AttributeError(f"Transformation method '{method_name_normalized}' not found.")

                    try:
                        if isinstance(params_normalized, list):
                            method(params_normalized)
                        elif isinstance(params_normalized, dict):
                            method(**params_normalized)
                        else:
                            method(params_normalized)
                        self.logger.info(f"Successfully applied '{method_name_normalized}'")
                    except Exception as e:
                        self.logger.error(f"Error applying transformation '{method_name_normalized}': {e}", exc_info=True)
                        raise

            self.logger.info("All transformations applied successfully.")
            return self.df


