import pandas as pd

class Merger:
    def __init__(self, logger):
        self.logger = logger

    def concatenate(self, dataframes: list) -> pd.DataFrame:
        self.logger.info("Concatenating dataframes...")
        return pd.concat(dataframes, ignore_index=True)

    def merge(self, dataframes: list, key_columns: list, how: str = 'inner') -> pd.DataFrame:
        """
        dataframes: list of DataFrames
        key_columns: list of columns to join on (must be present in all DataFrames)
        how: type of merge (inner, left, right, outer)
        """
        if not dataframes:
            self.logger.warning("No DataFrames provided for merge.")
            return pd.DataFrame()

        self.logger.info(f"Merging {len(dataframes)} dataframes using keys {key_columns} and how='{how}'...")
        final_df = dataframes[0]
        for df in dataframes[1:]:
            final_df = pd.merge(final_df, df, on=key_columns, how=how)
        return final_df
