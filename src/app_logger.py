import logging
import os
from datetime import datetime

class AppLogger:
    """
    A logger class that configures and provides a logger instance which logs
    messages to both the console and a uniquely named file determined by the
    current datetime. The logs are stored in a specified directory (default: logs).
    """

    def __init__(self, name: str = __name__, 
                 level: int = logging.DEBUG,
                 log_dir: str = 'logs'):
        """
        Initialize the AppLogger instance.

        :param name: Name of the logger (usually __name__).
        :param level: Logging level (e.g., logging.DEBUG, logging.INFO).
        :param log_dir: Directory where log files will be stored.
        """

        self.log_dir = log_dir

        # Create timestamp-based filename for uniqueness
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = os.path.join(self.log_dir, f"run_{timestamp}.log")

        self.logger = logging.getLogger(name)
        self.logger.setLevel(level)

        # Avoid adding multiple handlers if logger is re-initialized
        if not self.logger.handlers:
            # Ensure the log directory exists
            if log_dir and not os.path.exists(log_dir):
                os.makedirs(log_dir)

            # Create file handler
            file_handler = logging.FileHandler(log_file)
            file_handler.setLevel(level)

            # Create console handler
            console_handler = logging.StreamHandler()
            console_handler.setLevel(level)

            # Define log format
            formatter = logging.Formatter(
                "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S"
            )
            file_handler.setFormatter(formatter)
            console_handler.setFormatter(formatter)

            # Add handlers to the logger
            self.logger.addHandler(file_handler)
            self.logger.addHandler(console_handler)

    def get_logger(self) -> logging.Logger:
        """
        Returns the underlying logger instance.
        """
        return self.logger
