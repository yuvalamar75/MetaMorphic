import yaml
from yaml.parser import ParserError
from yaml.scanner import ScannerError

class ConfigParser:
    def __init__(self, config_path: str, logger):
        """
        :param config_path: Path to the YAML configuration file.
        :param logger: An instance of a logger object to use for logging.
        """
        self.config_path = config_path
        self._config = None
        self.logger = logger.getChild(self.__class__.__name__)
        self.logger.info(f"ConfigParser initialized with config path: {self.config_path}")

    def load_config(self):
        """
        Load and parse the YAML configuration file.
        """
        self.logger.info(f"Attempting to load configuration from: {self.config_path}")
        try:
            with open(self.config_path, 'r',encoding='utf-8') as f:
                self._config = yaml.safe_load(f)
            self.logger.info("Configuration loaded successfully.")
        except FileNotFoundError:
            self.logger.error(f"Configuration file not found: {self.config_path}")
            raise
        except (ParserError, ScannerError) as e:
            self.logger.error(f"Failed to parse YAML configuration. Error: {e}")
            raise
        except Exception as e:
            self.logger.error(f"An unexpected error occurred while loading config: {e}")
            raise

    # --------------------------
    #         SETTINGS
    # --------------------------
    def get_settings(self) -> str:
        """
        Retrieve the global output file path from the 'settings' section.
        Example:
          settings:
            output_file: "output/merged_data.csv"
        """
        self.logger.info("Retrieving output file setting from 'settings'...")
        settings = self._config.get('settings', {})
        output_file = settings.get('output_folder', None)
        self.logger.debug(f"Retrieved output file: {output_file}")
        return output_file

    def get_default_join_type(self) -> str:
        """
        Retrieve the global default join type from the 'settings' section.
        Example:
          settings:
            default_join_type: "left"
        If not found, defaults to 'inner'.
        """
        self.logger.info("Retrieving default join type from 'settings'...")
        settings = self._config.get('settings', {})
        default_join_type = settings.get('default_join_type', 'inner')
        self.logger.debug(f"Default join type: {default_join_type}")
        return default_join_type

    # --------------------------
    #           FILES
    # --------------------------
    def get_files(self) -> list:
        """
        Retrieve file definitions from the 'files' section.
        Each file entry may contain:
          - name (str): Unique reference name
          - path (str): Actual file path
          - keys (list): Columns that serve as join keys
          - transformations (dict): Optional transformations (e.g., select_columns, rename_columns, filter_rows)
          - filter_rows (dict): Optional filter rows (e.g., column, operator, values)
        Example:
          files:
            - name: "file3"
              path: "sample/batch1/file3.csv"
              keys: ["id"]
              transformations:
                select_columns: ["id", "customer_name", "email"]
              filter_rows:
                column: "status"
                operator: "is_in"
                values: ["active", "inactive"]
        Returns a list of dictionaries, each describing a file.
        """
        self.logger.info("Retrieving files configuration from 'files'...")
        raw_files = self._config.get('files', [])
        parsed_files = []
        for f in raw_files:
            # Extract known fields; transformations are optional
            file_info = {
                'name': f.get('name'),
                'path': f.get('path'),
                'sheet': f.get('sheet'),
                'transformations': f.get('transformations', {}),
                'filter_rows': f.get('filter_rows', {})
            }
            parsed_files.append(file_info)

        self.logger.debug(f"Parsed files: {parsed_files}")
        return parsed_files

    # --------------------------
    #           JOINS
    # --------------------------
    def get_joins(self) -> list:
        """
        Retrieve the list of join operations from the 'joins' section.
        Each join entry may contain:
          - source (str): The "base" dataset name
          - join_with (str): The dataset name to merge onto the base
          - join_type (str): The type of join (inner, left, right, outer). If missing, use default_join_type.
          - on (dict): A mapping of source dataset key -> join_with dataset key, e.g.:
              on:
                main_data: "id"
                orders: "id"
        Returns a list of dictionaries describing each join.
        """
        self.logger.info("Retrieving joins configuration from 'joins'...")
        raw_joins = self._config.get('joins', [])
        parsed_joins = []
        for j in raw_joins:
            join_info = {
                'source': j.get('source'),
                'join_with': j.get('join_with'),
                'join_type': j.get('join_type'),  # Could be None if not specified
                'join_on': j.get('join_on', {})
            }
            parsed_joins.append(join_info)

        self.logger.debug(f"Parsed joins: {parsed_joins}")
        return parsed_joins

    # --------------------------
    #   OPTIONAL: FULL SETTINGS
    # --------------------------
    def get_settings(self) -> dict:
        """
        (Optional) Retrieves the entire 'settings' dictionary for advanced usage.
        Could include additional items beyond output_file and default_join_type.
        """
        self.logger.info("Retrieving entire 'settings' dictionary...")
        settings = self._config.get('settings', {})
        self.logger.debug(f"Full settings: {settings}")
        return settings

if __name__ == "__main__":
    # Example usage
    from src.app_logger import AppLogger

    logger = AppLogger(name="ConfigParserDemo").get_logger()
    parser = ConfigParser(config_path="/Users/yuval/MetaMorphic/configs/first.yaml", logger=logger)
    try:
        parser.load_config()
        output_file = parser.get_output_file()
        default_join_type = parser.get_default_join_type()
        files = parser.get_files()
        joins = parser.get_joins()

        print("Output File:", output_file)
        print("Default Join Type:", default_join_type)
        print("Files:", files)
        print("Joins:", joins)

    except Exception as e:
        logger.error(f"An error occurred: {e}")
