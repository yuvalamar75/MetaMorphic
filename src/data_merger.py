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
        self.logger = logger.getChild(self.__class__.__name__)
        self.dfs_dict = dfs_dict
        self.join_configuration = join_configuration
        self.output_folder = output_folder
        
        # Create the output folder if it doesn't exist
        os.makedirs(self.output_folder, exist_ok=True)

    def concatenate(self, dataframes_names: list, ignore_index: bool = True) -> pd.DataFrame:
        """
        Concatenates multiple DataFrames from the dfs_dict.
        
        :param dataframes_names: List of dataframe names to concatenate
        :param ignore_index: Whether to reset the index in the result
        :return: Concatenated DataFrame
        """
        self.logger.info(f"Concatenating dataframes: {dataframes_names}")
        
        # Get the dataframes from dfs_dict
        dfs_to_concat = []
        for df_name in dataframes_names:
            df = self.dfs_dict.get(df_name)
            if df is None:
                self.logger.error(f"DataFrame '{df_name}' not found in dfs_dict.")
                raise ValueError(f"DataFrame '{df_name}' not found")
            dfs_to_concat.append(df)
        
        try:
            result = pd.concat(dfs_to_concat, ignore_index=ignore_index)
            self.logger.info(f"Successfully concatenated {len(dfs_to_concat)} dataframes")
            return result
        except Exception as e:
            self.logger.error(f"Error during concatenation: {e}")
            raise

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
        Now supports both merge and concatenate operations.
        """
        step = 1
        for join_step in self.join_configuration:
            operation_type = join_step.get("type", "merge")  # Default to merge if not specified
            output_name = join_step.get("output", f"join_step{step}")
            
            try:
                if operation_type == "merge":
                    # Existing merge logic
                    left_file = join_step.get("source")
                    right_file = join_step.get("join_with")
                    keys = join_step.get("join_on")
                    left_key = keys[0].get(left_file)
                    right_key = keys[1].get(right_file)
                    how = join_step.get("join_type", "inner")
                    
                    self.logger.info(f"Step {step}: Merging '{left_file}' with '{right_file}'")
                    
                    left_df = self.dfs_dict.get(left_file)
                    right_df = self.dfs_dict.get(right_file)
                    
                    if left_df is None or right_df is None:
                        raise ValueError(f"Required DataFrames not found: {left_file} or {right_file}")
                    
                    result_df = self.merge(left_df, right_df, left_key, right_key, how)
                    self.logger.info(f"Merged DataFrame shape: {result_df.shape[0]} rows, {result_df.shape[1]} columns")
                    self.dfs_dict[left_file] = result_df
                    final_left_file = left_file
                    
                elif operation_type == "concat":
                    # New concatenation logic
                    dataframes = join_step.get("dataframes", [])
                    ignore_index = join_step.get("ignore_index", True)
                    
                    self.logger.info(f"Step {step}: Concatenating dataframes: {dataframes}")
                    
                    result_df = self.concatenate(dataframes, ignore_index)
                    self.dfs_dict[output_name] = result_df
                    final_left_file = output_name
                
                else:
                    raise ValueError(f"Unknown operation type: {operation_type}")
                
                # Save intermediate result
                output_filename = f"step{step}_{output_name}.csv"
                output_path = os.path.join(self.output_folder, output_filename)
                result_df.to_csv(output_path, index=False, encoding='utf-8-sig')
                self.logger.info(f"Saved result to '{output_path}'")
                
            except Exception as e:
                self.logger.error(f"Error in step {step}: {e}")
                raise
            
            finally:
                step += 1
        
        return self.dfs_dict, final_left_file


