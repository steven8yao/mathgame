import logging
import os
import datetime

# This logger is for internal messages from app_logger.py itself
_internal_module_logger = logging.getLogger(__name__)

def create_file_logger(logger_name: str, log_file_full_path: str, level=logging.INFO):
    """
    Creates and configures a logger instance that writes to a specific file.

    :param logger_name: Name for the logger (e.g., 'sec4_logger').
    :param log_file_full_path: Full path to the log file.
    :param level: Logging level for the file handler.
    :return: Configured logging.Logger instance.
    """
    logger = logging.getLogger(logger_name)
    logger.setLevel(level) # Set logger to the lowest level it will handle

    # Prevent multiple handlers if this function is called again for the same logger name
    if logger.hasHandlers():
        logger.handlers.clear()

    # File Handler
    try:
        log_directory = os.path.dirname(log_file_full_path)
        if log_directory: # Ensure log_directory is not empty
            os.makedirs(log_directory, exist_ok=True)
        
        file_handler = logging.FileHandler(log_file_full_path)
        file_handler.setLevel(level)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
        _internal_module_logger.debug(f"File handler configured for logger '{logger_name}' at path '{log_file_full_path}'.")
    except Exception as e:
        _internal_module_logger.error(f"Failed to create file handler for logger '{logger_name}' at path '{log_file_full_path}': {e}", exc_info=True)
        # Fallback to basic console logging for this specific logger if file setup fails
        console_handler = logging.StreamHandler()
        console_handler.setLevel(level)
        error_formatter = logging.Formatter('%(asctime)s - %(name)s - ERROR_SETUP - %(levelname)s - %(message)s')
        console_handler.setFormatter(error_formatter)
        logger.addHandler(console_handler)
        logger.error(f"File logging setup failed for {log_file_full_path}. Using console for this logger.")
        
    logger.propagate = False # Prevent logs from going to the root logger if it has handlers

    return logger


class LogPrintHelper:
    """
    A helper class that uses a provided logger instance to log messages
    and also prints messages to the console, including the log level.
    """
    def __init__(self, logger: logging.Logger, print_to_console: bool = True):
        if not isinstance(logger, logging.Logger):
            # This case should ideally be avoided by ensuring a valid logger is passed.
            _internal_module_logger.error(f"LogPrintHelper initialized with an invalid logger: {type(logger)}. Using a fallback.")
            self.logger = logging.getLogger(f"fallback_logprint_{id(self)}") # Unique fallback logger name
            self.logger.setLevel(logging.WARNING)
            # Add a basic console handler to the fallback logger so messages are at least visible
            if not self.logger.hasHandlers():
                ch = logging.StreamHandler()
                ch.setFormatter(logging.Formatter('%(asctime)s - FALLBACK_LOGGER - %(levelname)s - %(message)s'))
                self.logger.addHandler(ch)
            self.logger.warning("LogPrintHelper was initialized with a non-Logger object. Logging may not work as intended.")
        else:
            self.logger = logger
        self.print_to_console = print_to_console

    def _log_and_print(self, level, *args, **kwargs):
        message = ' '.join(map(str, args))
        level_name = logging.getLevelName(level)

        self.logger.log(level, message)
        
        if self.print_to_console:
            print(f"[{level_name}] {message}")

    def info(self, *args, **kwargs): self._log_and_print(logging.INFO, *args, **kwargs)
    def warning(self, *args, **kwargs): self._log_and_print(logging.WARNING, *args, **kwargs)
    def error(self, *args, **kwargs): self._log_and_print(logging.ERROR, *args, **kwargs)
    def debug(self, *args, **kwargs): self._log_and_print(logging.DEBUG, *args, **kwargs)
    def critical(self, *args, **kwargs): self._log_and_print(logging.CRITICAL, *args, **kwargs)

# Example of how to set up a basic root logger if you want app_logger's internal messages to be visible
# logging.basicConfig(level=logging.DEBUG) # Uncomment for debugging app_logger.py itself

# # Create the global instance that other modules will import and use
# log_print = _LoggerHelper()

# --- Example Usage (for testing this file directly) ---
if __name__ == '__main__':
    # Define a test log file path
    current_dir = os.path.dirname(os.path.abspath(__file__))
    test_log_file = os.path.join(current_dir, 'test_app_logger.log')

    # Call setup_logging first!
    print(f"Attempting to set up logging to: {test_log_file}")
    setup_logging(test_log_file, level=logging.DEBUG) # Set to DEBUG to test log_print.debug()

    print("\n--- Testing log_print object ---")
    log_print.info("This is an info message from log_print.info().")
    log_print.warning("This is a warning message from log_print.warning().")
    log_print.error("This is an error message from log_print.error().")
    log_print.debug("This is a debug message from log_print.debug(). It should appear in the file if level is DEBUG.")
    log_print.critical("This is a critical message from log_print.critical().")

    # Test case: Logger not configured (if setup_logging was not called)
    # To test this, you'd have to comment out the setup_logging call above
    # and then calls to log_print methods would show the "Logger not configured" warning.
    # For example:
    # _is_configured = False # Simulate not configured for a moment (don't do this in real code)
    # _logger_instance = None
    # print("\n--- Testing log_print without setup (simulated) ---")
    # log_print.info("This message should trigger the 'not configured' warning.")

    print(f"\nCheck the log file: {test_log_file}")
    print("Also check console output for messages.")