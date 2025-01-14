import pandas as pd
import os

class DataMerger:
    def __init__(self, logger, dfs_dict, join_configuration, output_folder):
        """
        Initializes the DataMerger.

        :param logger: Logger object for logging information and warnings.
        :param dfs_dict: Dictionary containing initial DataFrames with their source names as keys.
        :param join_configuration: List of dictionaries specifying join operations.
        :param output_folder: Path to the folder where output CSV files will be saved.
        """
        self.logger = logger
        self.dfs_dict = dfs_dict
        self.join_configuration = join_configuration
        self.output_folder = output_folder
        
        # Create the output folder if it doesn't exist
        os.makedirs(self.output_folder, exist_ok=True)

    def concatenate(self, dataframes: list) -> pd.DataFrame:
        self.logger.info("Concatenating dataframes...")
        return pd.concat(dataframes, ignore_index=True)

    def merge(self, left_df: pd.DataFrame, right_df: pd.DataFrame, left_key: list ,right_key: list, how: str = 'inner') -> pd.DataFrame:
        """
        Merges two DataFrames on the specified keys.

        :param left_df: Left DataFrame.
        :param right_df: Right DataFrame.
        :param key_columns: List of columns to join on.
        :param how: Type of join ('inner', 'left', 'right', 'outer').
        :return: Merged DataFrame.
        """
        self.logger.info(f"Merging dataframes using keys {left_key}<->{right_key} and how='{how}'...")
        return pd.merge(left_df, right_df, left_on=left_key,right_on=right_key, how=how)

    def run_joins(self):
        """
        Executes the join operations as specified in the join_configuration.
        After each join, updates dfs_dict and saves the resulting DataFrame as a CSV file.
        """
        step = 1
        for join_step in self.join_configuration:
            left_file = join_step.get("source")
            right_file = join_step.get("join_with")
            keys = join_step.get("join_on")
            left_key = keys[0].get(left_file)
            right_key = keys[1].get(right_file)
            how = join_step.get("how", "inner")
            output_name = join_step.get("output", f"join_step{step}")

            self.logger.info(f"Step {step}: Joining '{left_key}' with '{right_key}' on keys {left_key}<->{right_key} using '{how}' join.")

            # Retrieve DataFrames from dfs_dict
            left_df = self.dfs_dict.get(left_file)
            right_df = self.dfs_dict.get(right_file)

            if left_df is None:
                self.logger.error(f"Left DataFrame '{left_file}' not found in dfs_dict.")
                continue
            if right_df is None:
                self.logger.error(f"Right DataFrame '{right_file}' not found in dfs_dict.")
                continue

            # Perform the merge
            try:
                merged_df = self.merge(left_df, right_df, left_key,right_key, how)
                self.dfs_dict[left_file] = merged_df
            except Exception as e:
                self.logger.error(f"Error merging step:{step} '{left_file}' and '{right_file} ': {e}")
                continue

            # Update dfs_dict with the new merged DataFrame
            self.dfs_dict[output_name] = merged_df
            self.logger.info(f"Join step {step} completed. New DataFrame '{output_name}' added to dfs_dict.")

            # Save the merged DataFrame to CSV
            output_filename = f"{left_file}_{right_file}_step{step}.csv"
            output_path = os.path.join(self.output_folder, output_filename)
            try:
                merged_df.to_csv(output_path, index=False)
                self.logger.info(f"Saved merged DataFrame to '{output_path}'.")
            except Exception as e:
                self.logger.error(f"Failed to save '{output_path}': {e}")

            finally:
            # Ensure that the step counter is incremented regardless of what happens above
                step += 1
        
        return self.dfs_dict,left_file