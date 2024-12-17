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
        self.logger = logger
        self.logger.info(f"ConfigParser initialized with config path: {self.config_path}")

    def load_config(self):
        """Load and parse the YAML configuration file."""
        self.logger.info(f"Attempting to load configuration from: {self.config_path}")
        try:
            with open(self.config_path, 'r') as f:
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

    def get_output_file(self) -> str:
        """Get the global output file path from settings."""
        self.logger.info("Retrieving output file setting...")
        output_file = self._config.get('settings', {}).get('output_file', 'output.csv')
        self.logger.debug(f"Retrieved output file: {output_file}")
        return output_file

    def get_default_join_type(self) -> str:
        """Get the default join type from settings."""
        self.logger.info("Retrieving default join type setting...")
        default_join_type = self._config.get('settings', {}).get('default_join_type', 'inner')
        self.logger.debug(f"Default join type: {default_join_type}")
        return default_join_type

    def get_files(self) -> list:
        """
        Get the list of file definitions.
        Returns a list of dicts.
        """
        self.logger.info("Retrieving files configuration...")
        files = self._config.get('files', [])
        self.logger.debug(f"Files configuration: {files}")
        return files

    def get_joins(self) -> list:
        """
        Get the list of join operations.
        """
        self.logger.info("Retrieving joins configuration...")
        joins = self._config.get('joins', [])
        self.logger.debug(f"Joins configuration: {joins}")
        return joins

if __name__ == "__main__":
    from src.app_logger import AppLogger
    logger = AppLogger(name="ConfigParser").get_logger()
    parser = ConfigParser("/Users/yuval/MetaMorphic/configs/first.yaml", logger)
    try:
        parser.load_config()
        print("Default Join Type:", parser.get_default_join_type())
        print("Files:", parser.get_files())
        print("Joins:", parser.get_joins())
    except Exception as e:
        # Errors are already logged by the logger
        pass
