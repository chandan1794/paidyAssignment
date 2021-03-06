# Paidy Data Science Engineer Assignment

Problem statement can be found at https://github.com/paidy/dse-interview


# Part 1
## Assumptions
1. The data format remains the same.
2. The frequency of data is hourly (Will try to make it configurable).
3. The CSV files are loaded on source(AWS S3) by 3rd party or as an output from some other source. 
In this project that is considered as a black box.
4. Every time the data is added using a new file or a group of new files.
5. No old files are modified in any way.
6. The CSV file name is following a constant format. (Might delete it later)
7. In the source folder there are only data files and nothing else.
8. The files path should follow the following pattern,
   ```
    yyyy/mm/dd/HH/some_uniform_name.csv
    Example:
       2021/01/01/23/file00001.csv
       2021/10/05/00/file00100.csv
   ```
9. All calculations of time is in UTC


## Steps
1. Cron Job: It is to trigger a Scanner process.
2. Scanner Process: It will scan the source of CSV files as per our frequency.
As per our assumption let us say it is an hour, then let us assume the Scanner runs
at 1200 hrs, then it will scan the filestore from _Latest last_modified_time_ to 1200 hrs.

    For some unfortunate reason, the process failed in between, then when we restart it,
    it will take the last time when we left scanning, by checking the **file_modified_time**
    and scan again.
    
    If the filestore cannot guarantee ordering of files according to their modified time, in
    that case, we can still list all for the past hour, and skip the ones which matches with
    the already present entries in the table.

    It will store the data in a metadata table for the scanner.
    The Scanner will scan files one by one, and add their sizes to not overreach the threshold,
    which we will set. Once close to it, it will create a new job and add a new entry to the
    ETL task metadata table.
    Also, will add a new job to the ETL, when there are no more new files to scan, after waiting
    for a certain threshold period.

3. ETL JOB: In this job we will be performing all the ETL tasks we are required to.
   
   This process will be triggered by a cron. Multiple ETLs can be used, because of the fact, that
   if we are running just one ETL process at a time, then only 1 job will execute at a time, but Scanner 
   can create multiple jobs. If the job creation exceeds the capabilities of 1 ETL, then we can add another ETL.

### Scanner Table Schema
**Id**:  Unique Id

**files**: String of comma separated full file uris

**latest_file_modified_time**: Latest last modified time in the whole job. 
This will be used to find out when the latest file is uploaded and then
will be considered to download the data or not.

**total_size_in_bytes**: Sum of size of all the file in bytes

**created_time**: First time when the scanner created this row

**modified_time**: When the status of the row changed last
 
**status**: Status of the job. In our case, we just need to have two status.
1. _NONE_: Dummy job, there will be just one such job with this status in the whole table.
2. _SENT_FOR_ETL_: When the job is ready for the ETL
3. _PROCESSING_: When the job is under processing, means taken up by the ETL process
4. _LOADED_: When the ETL process loads the data to the database
5. _FAILED_: If the process fails
 
**failure_msg**: If failed, we can add the stacktrace here for easy viewing.

## Software Requirements
1. Python 3.7+
2. MySQL

## Getting Started
1. Install MySQL
2. Copy the _config.yaml.example_ and create _config.yaml_
3. Add/Update values in the config file
4. Create the following two databases in MySQL, and make sure the name of 
Database in the _config.yaml_ is same as in the commands
    ```
    CREATE DATABASE etl_pipeline_metadata CHARACTER SET utf8;
    CREATE DATABASE raw_data CHARACTER SET utf8;
    ```
5. Install the python requirements. (Run commands at the root)
    ```bash
    pip install -r requirements.txt
    pip install .
    ```
6. Run the setup script
   ```
    python3 setup_pipeline.py
   ```
    1. This is to setup the database in the system you are running. _Make sure you have MySQL installed_.
    2. Check if the S3 connection is working or not
7. Update the _env.sh_ as per the requirement
8. Set the following variables
   ```bash
   export PROJECT_DIR=/path/to/project/root
   ```
9. Launch the pipeline
   ```bash
   bash deploy_pipeline.sh
   ```

## Flow of the Solution:
1. There will be **just one Scanner Job** Running. Which will be looking at the 
S3 bucket for the possible new files according to our frequency.
Whenever it find new files, it will group those files into one single job
according to the threshold size we set for the job.

    A new job will look something like this

    ```sql
    id files latest_file_modified_time total_size_in_bytes created_time modified_time status failure_msg
    1 s3://credit-risk-data/2021/10/08/06/xaa,s3://credit-risk-data/2021/10/08/06/xab 2021-10-09 12:28:49 1494300 2021-10-09 17:43:46, 2021-10-09 17:43:46 SENT_FOR_ETL 
    ```
    > The reason why there should only be one scanner job is, because if there are multiple,
    many can end up scanning same files and duplicating the data in the process.

2. There can be multiple ETLs(**not yet implemented**), which are running.
    1. The ETL will take up the job with the status **SENT_FOR_ETL** and update the
    status to *PROCESSING*
    2. Download the files from the S3 for the given job
    3. Data Cleaning
        1. Imposing schema
        2. Adding correct null values
    4. Loading the data in the Reporting Database
        1. If the process fails, it throws an error, and update the job status to _FAILED_
        and add the stacktrace to the Scanner table's _failure_msg_ field
        2. If the process succeeds, it shows a success msg, and update the job status to
        _LOADED_
    5. The steps 1-4 are repeated until there are jobs with status **SENT_FOR_ETL** in 
    the SCANNER Table.


## KEEP IN MIND!
1. There should only be 1 Scanner. I have designed the deployment script with that
in mind. If you explicitly run the python script, it can cause duplication in the final data.
2. You have to kill the processes manually. Below commands will give you the list of 
   processes running
   ```bash
    ps -ef | grep "start_scanner.py" | grep -v grep
    ps -ef | grep "start_etl.py" | grep -v grep
   ```
3. Once a process is failed, to re-run it, you have to modify the entry in the db itself.
   Set the status of the row to `SENT_FOR_ETL`.

### Todo:
1. [x] Create a central script, which can 
    1. [x] Launch _1 Scanner_
    2. [x] Launch _N ETLs_
2. [x] Handle multiple ETLs
3. [ ] Retry a failed job


## References:
1. YAML: https://www.cloudbees.com/blog/yaml-tutorial-everything-you-need-get-started
2. CRUD in SQLAlchemy: https://overiq.com/sqlalchemy-101/defining-schema-in-sqlalchemy-orm/
3. CSV from S3: https://dev.to/shihanng/how-to-read-csv-file-from-amazon-s3-in-python-4ee9


# Part 2:
For part 2, I have performed all the EDA in a Jupyter Notebook. 
Which is `eda_of_loan_applications.ipynb`. 
It requires `sample_data.csv` at the same level.

## Tasks
1. Reading the Data File
2. Data Types
3. Statistical Description of columns
4. Balance/Imbalance Analysis
5. Correlation Analysis
6. Outlier Detection
7. Data Imputation
8. Feature Engineering
9. Final Words
