# Paidy Data Science Engineer Assignment

Problem statement can be found at https://github.com/paidy/dse-interview

## Assumptions
1. The data format remains same.
2. The frequency of data is hourly (Will try to make it configurable).
3. The CSV files is loaded on source by 3rd party or as an output from some other source. 
In this project that is considered as a black box.
4. Every time the data is added using a new file or a group of new files.
5. No old files are modified in anyway.
6. The CSV file name is following a constant format. (Might delete it later)

## Steps
1. Cron Job: It is to trigger a Scanner process.
2. Scanner Process: It will scan the source of CSV files as per our frequency.
As per our assumption let us say it is an hour, then let us assume the Scanner runs
at 1200 hrs, then it will scan the filestore from 1100 to 1200 hrs.

    For some unfortunate reason, the process failed in between, then when we restarts it,
    it will take the last time where we left scanning, by checking the **file_modified_time**
    and scan again.
    
    If the filestore cannot guarantee ordering of files according to their modified time, in
    that case, we can still list all for the past hour, and skip the ones which matches with
    the already present entries in the table.

    It will store the data in a metadata table for the scanner.
    The Scanner will scan files one by one, and add their sizes to not over reach the threshold,
    which we will set. Once close to it, it will create a new job and add a new entry to the
    ETL task metadata table.
    Also will add a new job to the ETL, when there are no more new files to scan, after waiting
    for a certain threshold period.
    
    ### Scanner Table Schema
    **id**:  Unique Id
    
    **file_name**: Full file uri
    
    **file_modified_time**: Modified time of the file. This will be used to find
    out when the file is uploaded and then will be considered to download the data
    or not.
    
    **size_in_bytes**: Size of the file in bytes
    
    **created_time**: First time when the scanner created this row
    
    **modified_time**: When the status of the row changes.
     
     **status**: Status of the file. In our case, we just need to have two status.
     
     1. _SCANNED_: When the file is officially in the system to process
     2. _SENT_FOR_ETL_: When the file is sent for processing

3. ETL JOB: In this job we will be performing all the ETL tasks we are required to.
   
   This process will be triggered by a cron of faster frequency then the Scanner. Because of the fact, that
   if we are running just one ETL process at a time, then only 1 job will execute at a time, but Scanner 
   can create multiple jobs. If the job creation exceeds the capabilities of 1 ETL, then we can add another ETL.
   
   > Will not add the feature to handle multiple ETL processes at the moment.

   ### ETL Task Table Schema
   **id**: Unique Id
   
   **files**: String of comma separated full file uris
  
   **created_time**: Time when this job was created by Scanner
   
   **modified_time**: Time when the status of the job changes
   
   **Status**: There can possibly be three status
   
   1. _NEW_: It is a new job and has not processed yet
   2. _PROCESSING_: When the job is under processing, means taken up by the ETL process
   3. _LOADED_: When the ETL process loads the data
   4. _FAILED_: If the process fails
   
   **FAILURE_MSG**: If failed, we can add the stacktrace here for easy viewing.
