import argparse
import logging
import os 
from src.app_logger import AppLogger
from src.config_parser import ConfigParser
from src.data_loader import DataLoader
from src.transformer import Transformer
from src.merger import Merger
import pandas as pd

def main(config_path: str):
    # Initialize logger (adjust name/level as needed)
    logger = AppLogger(name="MainScript", level=logging.INFO).get_logger()

    logger.info(f"Loading configuration from {config_path}")

    # Create and load the configuration
    config_parser = ConfigParser(config_path, logger)
    config_parser.load_config()

    # Retrieve values from the config
    output_file = config_parser.get_output_file()
    default_join_type = config_parser.get_default_join_type()
    files = config_parser.get_files()
    joins = config_parser.get_joins()

    # For demonstration, log them (or do further processing)
    logger.info(f"Output file: {output_file}")
    logger.info(f"Default join type: {default_join_type}")
    logger.info(f"Files definition:\n{files}")
    logger.info(f"Join steps:\n{joins}")

    # Initialize helper classes
    data_loader = DataLoader(logger)
    transformations = Transformer(logger)
    merger = Merger(logger)

    # Load data
    dataframes = []
    key_columns_all = []
    print(files)
    for inp in files:
        print(inp)
        file_path = inp['file']
        file_type = inp['type']
        sheet = inp.get('sheet', None)
        key_cols = inp.get('key_columns', [])

        df = data_loader.load_data(file_path, file_type, sheet)
        print(df)
    #     # Apply transformations if needed
    #     if columns_to_standardize:
    #         df = transformations.standardize_id_column(df, columns_to_standardize)

    #     dataframes.append(df)
    #     key_columns_all.append(key_cols)

    # # Determine operation
    # op_type = operation.get('type', 'concat')  # default concat if not specified

    # if op_type == 'concat':
    #     final_df = merger.concatenate(dataframes)
    # elif op_type == 'merge':
    #     # Validate that all input have the same key columns
    #     # For simplicity, assume the first set of key columns is the "official" one
    #     primary_keys = key_columns_all[0] if key_columns_all else []
    #     if not primary_keys:
    #         logger.error("No key columns provided for a merge operation.")
    #         raise ValueError("Merge operation requires 'key_columns' in the configuration.")

    #     # Make sure all dataframes have these key columns specified
    #     for i, kc in enumerate(key_columns_all):
    #         if kc != primary_keys:
    #             logger.error(f"Dataframe at index {i} does not have matching key_columns. Expected {primary_keys}, got {kc}.")
    #             raise ValueError("All dataframes must share the same key columns for merging.")

    #     how = operation.get('how', 'inner')
    #     final_df = merger.merge(dataframes, primary_keys, how=how)
    # else:
    #     logger.error(f"Unknown operation type: {op_type}")
    #     raise ValueError(f"Unknown operation type: {op_type}")

    # # Write output
    # out_file = output_cfg['file']
    # out_type = output_cfg.get('type', 'csv')
    # logger.info(f"Writing output to {out_file} as {out_type}")

    # if out_type == 'csv':
    #     delimiter = output_cfg.get('delimiter', ',')
    #     final_df.to_csv(out_file, index=False, sep=delimiter)
    # elif out_type == 'xlsx':
    #     final_df.to_excel(out_file, index=False)
    # else:
    #     logger.error(f"Unknown output file type: {out_type}")
    #     raise ValueError(f"Unknown output file type: {out_type}")

    # logger.info("Processing completed successfully.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Data merging application")
    parser.add_argument("--config", required=True, help="Path to the configuration YAML file")
    args = parser.parse_args()
    main(args.config)
