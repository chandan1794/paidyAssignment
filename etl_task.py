"""
Module to handle the ETL
"""
import yaml
from db_helper.etl_metadata_database import ETLMetadataDatabaseConnector
from db_helper.reporting_database import ReportingDatabaseConnector, LoanApplicationsTable
from db_helper.database_connector import DatabaseConnector
from s3_helper import S3Helper
from config_data_classes import DatabaseConfig, S3Config, ETLConfig
import constants
import logging
import traceback

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class ETLTask:
    """
    Class to handle all ETL related tasks
    """
    def __init__(self,
                 etl_db_config: DatabaseConfig,
                 reporting_db_config: DatabaseConfig,
                 s3_config: S3Config,
                 etl_config: ETLConfig):
        """
        Initialising connection to S3, ETL Database and Reporting Database
        :param etl_db_config: ETL database config object
        :param reporting_db_config: Reporting database config object
        :param s3_config: S3 config Object
        :param etl_config: ETL tasks related object
        """

        self.etl_db = ETLMetadataDatabaseConnector(db_name=etl_db_config.DATABASE_NAME,
                                                   host=etl_db_config.HOST,
                                                   port=etl_db_config.PORT,
                                                   user=etl_db_config.USERNAME,
                                                   password=etl_db_config.PASSWORD)

        self.reporting_db = ReportingDatabaseConnector(db_name=reporting_db_config.DATABASE_NAME,
                                                       host=reporting_db_config.HOST,
                                                       port=reporting_db_config.PORT,
                                                       user=reporting_db_config.USERNAME,
                                                       password=reporting_db_config.PASSWORD)

        self.s3_helper = S3Helper(bucket_name=s3_config.BUCKET,
                                  access_key=s3_config.AWS_ACCESS_KEY,
                                  secret_key=s3_config.AWS_SECRET_KEY)

        self.etl_config = etl_config

    def clean_data(self, row):
        """
        Function to clean the data. This is the only place where custom code is required if file changes in future.
        :param row: row of data
        :return: cleaned row of data
        """
        cleaned_row = {}
        try:
            cleaned_row[""] = DatabaseConnector.cast(LoanApplicationsTable.id.type, row[""])
        except ValueError as _:
            raise TypeError(f"Id in wrong format {row['']}")

        try:
            cleaned_row["SeriousDlqin2yrs"] = DatabaseConnector.cast(LoanApplicationsTable.serious_dlqin_2_yrs.type,
                                                                     row["SeriousDlqin2yrs"])
        except ValueError as _:
            cleaned_row["SeriousDlqin2yrs"] = None

        try:
            cleaned_row["RevolvingUtilizationOfUnsecuredLines"] = DatabaseConnector.cast(
                LoanApplicationsTable.revolving_utilization_of_unsecured_lines.type,
                row["RevolvingUtilizationOfUnsecuredLines"])
        except ValueError as _:
            cleaned_row["RevolvingUtilizationOfUnsecuredLines"] = None

        try:
            cleaned_row["age"] = DatabaseConnector.cast(LoanApplicationsTable.age.type, row["age"])
        except ValueError as _:
            cleaned_row["age"] = None

        try:
            cleaned_row["NumberOfTime30-59DaysPastDueNotWorse"] = DatabaseConnector.cast(
                LoanApplicationsTable.number_of_time_30_59_days_past_due_not_worse.type,
                row["NumberOfTime30-59DaysPastDueNotWorse"])
        except ValueError as _:
            cleaned_row["NumberOfTime30-59DaysPastDueNotWorse"] = None

        try:
            cleaned_row["DebtRatio"] = DatabaseConnector.cast(LoanApplicationsTable.debt_ratio.type, row["DebtRatio"])
        except ValueError as _:
            cleaned_row["DebtRatio"] = None

        try:
            cleaned_row["MonthlyIncome"] = DatabaseConnector.cast(LoanApplicationsTable.monthly_income.type,
                                                                  row["MonthlyIncome"])
        except ValueError as _:
            cleaned_row["MonthlyIncome"] = None

        try:
            cleaned_row["NumberOfOpenCreditLinesAndLoans"] = DatabaseConnector.cast(
                LoanApplicationsTable.number_of_open_credit_lines_and_loans.type,
                row["NumberOfOpenCreditLinesAndLoans"])
        except ValueError as _:
            cleaned_row["NumberOfOpenCreditLinesAndLoans"] = None

        try:
            cleaned_row["NumberOfTimes90DaysLate"] = DatabaseConnector.cast(
                LoanApplicationsTable.number_of_time_90_days_late.type,
                row["NumberOfTimes90DaysLate"])
        except ValueError as _:
            cleaned_row["NumberOfTimes90DaysLate"] = None

        try:
            cleaned_row["NumberRealEstateLoansOrLines"] = DatabaseConnector.cast(
                LoanApplicationsTable.number_real_estate_loans_or_lines.type,
                row["NumberRealEstateLoansOrLines"])
        except ValueError as _:
            cleaned_row["NumberRealEstateLoansOrLines"] = None

        try:
            cleaned_row["NumberOfTime60-89DaysPastDueNotWorse"] = DatabaseConnector.cast(
                LoanApplicationsTable.number_of_times_60_89_days_past_due_not_worse.type,
                row["NumberOfTime60-89DaysPastDueNotWorse"])
        except ValueError as _:
            cleaned_row["NumberOfTime60-89DaysPastDueNotWorse"] = None

        try:
            cleaned_row["NumberOfDependents"] = DatabaseConnector.cast(LoanApplicationsTable.number_of_dependents.type,
                                                                       row["NumberOfDependents"])
        except ValueError as _:
            cleaned_row["NumberOfDependents"] = None

        return cleaned_row

    def run(self):
        """
        Function to run the whole ETL pipeline. Since ETL processes, ONE JOB at a time,
        it runs until there are no jobs in the Scanner Table with the status SENT_FOR_ETL.
        :return: None
        """
        while True:
            # 1. Fetch one job at a time
            etl_job_row = self.etl_db.get_latest_etl_job()
            logging.log(logging.INFO, f"Started the following ETL job: {etl_job_row}")

            if etl_job_row is not None:
                # 2. Split files and download all the files from S3
                s3_urls = etl_job_row.files.split(constants.MULTI_FILE_PATH_SEPARATOR)

                new_rows: [LoanApplicationsTable] = []
                for s3_url in s3_urls:
                    logging.log(logging.INFO, f"Streaming data from the {s3_url}")
                    for row in self.s3_helper.read_csv(s3_url):
                        try:
                            # 3. Clean data
                            cleaned_row = self.clean_data(row)
                        except TypeError as err:
                            logging.log(logging.WARNING, err)
                            logging.log(logging.WARNING,
                                        f"Skipping following row due to the above mentioned error: {row}")
                            continue

                        if "NA" in row.values():
                            logging.log(logging.DEBUG, f"Original Row: {row}")
                            logging.log(logging.DEBUG, f"Cleaned Row: {cleaned_row}")

                        # 4. Write to MySql
                        reporting_row = LoanApplicationsTable(
                            id=cleaned_row[""],
                            serious_dlqin_2_yrs=cleaned_row["SeriousDlqin2yrs"],
                            revolving_utilization_of_unsecured_lines=
                            cleaned_row["RevolvingUtilizationOfUnsecuredLines"],
                            age=cleaned_row["age"],
                            number_of_time_30_59_days_past_due_not_worse=
                            cleaned_row["NumberOfTime30-59DaysPastDueNotWorse"],
                            debt_ratio=cleaned_row["DebtRatio"],
                            monthly_income=cleaned_row["MonthlyIncome"],
                            number_of_open_credit_lines_and_loans=cleaned_row["NumberOfOpenCreditLinesAndLoans"],
                            number_of_time_90_days_late=cleaned_row["NumberOfTimes90DaysLate"],
                            number_real_estate_loans_or_lines=cleaned_row["NumberRealEstateLoansOrLines"],
                            number_of_times_60_89_days_past_due_not_worse=
                            cleaned_row["NumberOfTime60-89DaysPastDueNotWorse"],
                            number_of_dependents=cleaned_row["NumberOfDependents"]
                        )
                        new_rows.append(reporting_row)
                try:
                    self.reporting_db.create_new_jobs(new_rows)
                    self.etl_db.mark_downloading_from_s3_success(job_id=etl_job_row.id)
                    logging.log(logging.INFO, f"Successfully loaded rows from {s3_urls} to database.")
                    logging.log(logging.INFO, f"Successfully processed ETL Job with ID: {etl_job_row.id}")
                except:
                    # If for some reason the upload fails, we should mark the job as failed too
                    self.etl_db.mark_downloading_from_s3_failed(job_id=etl_job_row.id,
                                                                err_msg=traceback.format_exc()[:4096])
                    logging.exception(traceback.format_exc())
                    continue
            else:
                logging.log(logging.INFO, "No more ETL Jobs to process.")
                break


if __name__ == "__main__":
    # Importing all the configurations
    with open("config.yaml", "r") as conf_file:
        config = yaml.safe_load(conf_file)

    _etl_db_config = DatabaseConfig(**config["METADATA_DATABASE"])
    _reporting_db_config = DatabaseConfig(**config["REPORTING_DATABASE"])
    _s3_config = S3Config(**config["S3"])
    _etl_config = ETLConfig(**config["ETL"])

    ETLTask(etl_db_config=_etl_db_config,
            reporting_db_config=_reporting_db_config,
            s3_config=_s3_config,
            etl_config=_etl_config).run()
