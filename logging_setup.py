"""
Module to create basic setup for logging
"""
import logging


def get_logger(log_level=logging.INFO, process_name="%(name)s"):
    logging.basicConfig(level=log_level,
                        format=f'%(asctime)s - {process_name} - %(process)d - %(levelname)s - %(message)s')
    return logging
