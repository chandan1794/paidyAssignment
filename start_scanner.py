"""
Module to run the Scanner Task.
Once this script starts executing, it will run until the process is killed externally.
The CRON and SLEEP time is setup in the config.yaml.
It matches the CRON, and if the CRON matches, it executes the SCANNER task.
Between two consecutive Scanner CRON check, the process will sleep for CONSECUTIVE_EXECUTIONS_DELAY_IN_SECOND seconds.
"""

import yaml
from config_data_classes import DatabaseConfig, S3Config, ETLConfig, PipelineSettings
from pipeline_tasks import ScannerTask
from croniter import croniter
from datetime import datetime as dt
import constants
from time import sleep
from logging_setup import get_logger

logging = get_logger()


if __name__ == "__main__":
    logging.info("Scanner Deployed!")
    try:
        # Importing all the configurations
        with open("config.yaml", "r") as conf_file:
            config = yaml.safe_load(conf_file)

            _pipeline_settings = PipelineSettings(**config["PIPELINE_SETTINGS"])
            _etl_db_config = DatabaseConfig(**config["METADATA_DATABASE"])
            _s3_config = S3Config(**config["S3"])
            _etl_config = ETLConfig(**config["ETL"])

            while True:
                now = dt.now(tz=constants.TZ).replace(second=0).replace(microsecond=0)
                logging.info(f"Scanner Now: {now}")

                # Starting Scanner tasks
                if croniter.match(_pipeline_settings.SCANNER_CRON, now):
                    logging.info(f"Scanner Job started @{now}!")
                    # There should only be 1 Scanner
                    ScannerTask(etl_db_config=_etl_db_config,
                                s3_config=_s3_config,
                                etl_config=_etl_config).run()
                else:
                    logging.info("Scanner Cron hasn't match yet.")

                sleep(_pipeline_settings.CONSECUTIVE_EXECUTIONS_DELAY_IN_SECOND)

    except KeyError as err:
        raise err
