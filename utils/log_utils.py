import logging
import os
import sys
import threading
from logging.handlers import RotatingFileHandler
from typing import Optional

# Configure default logging parameters
DEFAULT_LOG_FORMAT = '%(asctime)s | %(levelname)s | %(threadName)s | %(message)s'
DEFAULT_LOG_LEVEL = logging.INFO
DEFAULT_MAX_BYTES = 10 * 1024 * 1024  # 10 MB
DEFAULT_BACKUP_COUNT = 5

def setup_logger(logfile: Optional[str] = None, level: int = DEFAULT_LOG_LEVEL) -> None:
    """
    Set up a logger with console and file handlers.
    
    Args:
        logfile: Path to log file. If None, logging will only be to console.
        level: Logging level to use for both handlers.
        
    Returns:
        None
    """
    # Create logger
    logger = logging.getLogger()
    logger.setLevel(level)
    
    # Clear existing handlers to avoid duplicates
    if logger.handlers:
        logger.handlers.clear()
    
    # Create formatter
    formatter = logging.Formatter(DEFAULT_LOG_FORMAT)
    
    # Add console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # Add file handler if logfile provided
    if logfile:
        # Ensure the directory exists
        os.makedirs(os.path.dirname(os.path.abspath(logfile)), exist_ok=True)
        
        # Create rotating file handler
        file_handler = RotatingFileHandler(
            logfile,
            maxBytes=DEFAULT_MAX_BYTES,
            backupCount=DEFAULT_BACKUP_COUNT
        )
        file_handler.setLevel(level)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    # Log setup completion
    logger.info(f"Logging initialized. Level: {logging.getLevelName(level)}")
    if logfile:
        logger.info(f"Log file: {os.path.abspath(logfile)}")

def get_logger(name: str = None) -> logging.Logger:
    """
    Get a logger with the given name.
    
    Args:
        name: Name of the logger to get. If None, returns the root logger.
        
    Returns:
        Logger instance
    """
    return logging.getLogger(name)

def set_thread_name(name: str) -> None:
    """
    Set the name of the current thread.
    
    Args:
        name: Name to set for the current thread
        
    Returns:
        None
    """
    threading.current_thread().name = name
    logging.debug(f"Thread name set to: {name}") 