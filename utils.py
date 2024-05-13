import os
import inspect
import logging


def setup_logging(local_log_file_name):
    """
    Sets up the logging configuration for the application, specifying the file where logs should be stored,
    the logging level, and the format of log messages. This setup ensures that all log messages are written
    to a specified file in a consistent format, making them easier to read and analyze.

    Args:
    - local_log_file_name (str): The name of the file where log messages will be stored.
    """
    print("Initiating log configurations..")
    logging.basicConfig(level=logging.INFO, filename=local_log_file_name, filemode='w',
                        format='%(asctime)s - %(levelname)s - %(message)s')


def log_and_print_error_message(error_message):
    """
        Logs an error message and prints it to the console.

        Args:
        - error_message (str): The error message to be logged and printed.
    """
    logging.error(error_message)
    print(f"Error: {error_message}")


def print_module_name():
    """
    Prints the name of the module from which this function is called. It uses the inspect module to trace the call
    stack and find the caller's module name.
    """
    # Stack inspection to fetch the caller's file path
    frame = inspect.currentframe()
    try:
        # Move back to the caller's stack frame
        caller_frame = frame.f_back
        filename = os.path.basename(caller_frame.f_globals['__file__'])
        print(f"\n##### {filename} Module #####\n")
    finally:
        # Clean up the frame to prevent reference cycles
        del frame
