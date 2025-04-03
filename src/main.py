import argparse
import logging
import os 
from app_logger import AppLogger
from config_parser import ConfigParser
from data_loader import DataLoader
from transformer import Transformer
from data_merger import DataMerger
import pandas as pd

def main(config_path):
    # Initialize logger (adjust name/level as needed)
    logger = AppLogger(name="MainScript", level=logging.INFO).get_logger()

    logger.info(f"Loading configuration from {config_path}")

    # Create and load the configuration
    config_parser = ConfigParser(config_path, logger)
    config_parser.load_config()

    # Retrieve values from the config
    settings= config_parser.get_settings()
    output_folder = settings.get('output_folder')
    output_file_name = settings.get('output_file_name')

    files_config = config_parser.get_files()
    joins_config = config_parser.get_joins()

    # For demonstration, log them (or do further processing)
    logger.info(f"Output file: {output_folder}")
    logger.info(f"Files definition:\n{files_config}")
    logger.info(f"Join steps:\n{joins_config}")

    # Initialize helper classes
    data_loader = DataLoader(logger)
    

    # Load data
    files_procssed = {}
   
    #load data
    #perforome transformations
    #add to dict
    for inp in files_config:
        file_name = inp['name']
        file_path = inp['path']
        file_type = file_path.rsplit(".",1)[-1]
        transformation_config = inp.get('transformations', None)
        sheet_name = inp.get('sheet', None)
        df = data_loader.load_data(file_path,file_type,sheet_name)
        input_file_name = os.path.basename(file_path)
        logger.info(f"Loaded file '{input_file_name}-{sheet_name}' with {df.shape[0]} rows")
        transformations = Transformer(logger,df)
        df_p = transformations.run_transformations(transformation_config)
        files_procssed[file_name] = df_p
    # Log the loaded and transformed files
    logger.info("Files loaded and transformed:")
    for file_name, df in files_procssed.items():
        logger.info(f"{file_name}: {df.shape[0]} rows, {df.shape[1]} columns")

    merger = DataMerger(logger,files_procssed,joins_config,output_folder)    
    merge_files_dict,left_file = merger.run_joins()
    df_final_output_file = merge_files_dict[left_file]
    
    
    output_file_type = output_file_name.rsplit(".",1)[-1]
    final_output_file_path = os.path.join(output_folder,output_file_name)
    if output_file_type == 'csv':
        df_final_output_file.to_csv(final_output_file_path, index=False,encoding='utf-8-sig')
        logger.info(f"final file saved in : {output_file_type}")
    elif output_file_type == 'xlsx':
        df_final_output_file.to_excel(final_output_file_path, index=False)
        logger.info(f"final file saved in : {output_file_type}")
    else:
        logger.error(f"Unknown output file type: {output_file_type}")
        raise ValueError(f"Unknown output file type: {output_file_type}")

    logger.info("Processing completed successfully.")




if __name__ == "__main__":
    #parser = argparse.ArgumentParser(description="Data merging application")
    #parser.add_argument("--config", required=True, help="Path to the configuration YAML file")
    #args = parser.parse_args()
    #main(args.config)
    config_path = '/Users/yuval/Documents/projects/MetaMorphic/configs/test_real_data.yaml'
    main(config_path)
