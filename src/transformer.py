import pandas as pd
import unicodedata

class Transformer:
    def __init__(self, logger,df):
        self.logger = logger.getChild(self.__class__.__name__)
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
    
    def select_columns(self, columns: list) -> pd.DataFrame:
        """Keeps only the specified columns in the DataFrame."""
        self.logger.info(f"slice data frame : before sliceing {self.df.shape[1]} columns")
        missing_columns = [col for col in columns if col not in self.df.columns]
        if missing_columns:
            self.logger.error(f"Columns not found in DataFrame: {missing_columns}")
            raise ValueError(f"Columns not found: {missing_columns}")
        
        self.df = self.df[columns]
        self.logger.info(f"Retained columns: {self.df.shape[1]}")
        return self.df
    
    def filter_rows(self, column: str, operator: str, values: list) -> pd.DataFrame:
        """
        Filter rows based on whether values in a column are in or not in a list of values.
        
        :param column: The column to filter on
        :param operator: Either 'is_in' or 'not_in'
        :param values: List of values to filter by
        :return: Filtered DataFrame
        """
        if column not in self.df.columns:
            self.logger.error(f"Column '{column}' not found in DataFrame.")
            raise ValueError(f"Column '{column}' not found.")

        self.logger.info(f"Filtering rows where {column} {operator} {values}")
        
        original_count = len(self.df)
        
        try:
            if operator == "is_in":
                self.df = self.df[self.df[column].isin(values)]
            elif operator == "not_in":
                self.df = self.df[~self.df[column].isin(values)]
            else:
                self.logger.error(f"Invalid operator '{operator}'. Must be 'is_in' or 'not_in'.")
                raise ValueError(f"Invalid operator '{operator}'. Must be 'is_in' or 'not_in'.")
            
            filtered_count = len(self.df)
            self.logger.info(f"Filtered {original_count - filtered_count} rows. {filtered_count} rows remaining.")
            
            return self.df
            
        except Exception as e:
            self.logger.error(f"Error filtering rows: {e}")
            raise

    def drop_duplicates(self, columns: list, keep: str = 'first') -> pd.DataFrame:
        """
        Drop duplicate rows based on specified columns.
        
        :param columns: List of columns to consider for duplicates
        :param keep: Which duplicate to keep ('first', 'last', or False to drop all)
        :return: DataFrame with duplicates removed
        """
        if not columns:
            self.logger.error("Empty columns list provided")
            raise ValueError("Columns list cannot be empty")
            
        missing_columns = [col for col in columns if col not in self.df.columns]
        if missing_columns:
            self.logger.error(f"Columns not found in DataFrame: {missing_columns}")
            raise ValueError(f"Columns not found: {missing_columns}")
            
        original_count = len(self.df)
        self.df = self.df.drop_duplicates(subset=columns, keep=keep)
        dropped_count = original_count - len(self.df)
        
        self.logger.info(f"Dropped {dropped_count} duplicate rows based on columns: {columns}")
        return self.df

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

    def add_group_order(self, group_columns: list, order_column_name: str = 'order') -> pd.DataFrame:
        """
        Sort values by specified columns and assign order numbers within groups.
        
        :param group_columns: List of columns to group and sort by
        :param order_column_name: Name of the new column that will contain the order numbers (default: 'order')
        :return: DataFrame with new order column
        """
        self.logger.info(f"Adding group order based on columns: {group_columns}")
        
        # Validate input columns
        if not group_columns:
            self.logger.error("Empty group columns list provided")
            raise ValueError("Group columns list cannot be empty")
        
        missing_columns = [col for col in group_columns if col not in self.df.columns]
        if missing_columns:
            self.logger.error(f"Columns not found in DataFrame: {missing_columns}")
            raise ValueError(f"Columns not found: {missing_columns}")
        
        try:
            # Sort the DataFrame by the specified columns
            self.df = self.df.sort_values(by=group_columns)
            
            # Add order numbers within groups
            self.df[order_column_name] = self.df.groupby(group_columns, dropna=False).cumcount() + 1
            
            self.logger.info(f"Successfully added order column '{order_column_name}'")
            return self.df
            
        except Exception as e:
            self.logger.error(f"Error adding group order: {e}")
            raise


