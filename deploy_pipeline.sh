# Script to run ETL and Scanner tasks
# We can comment out Scanner and ETLs and run them in separate terminals
#
# Script will require you to set the PROJECT_DIR variable first.
# The path is path to the project root
# Run
# $export PROJECT_DIR=path/to/root
#
# The script will source the env.sh from the root to get any setup environment.
# The script assumes the environment is setup completely. This is just a launch script.

# Project directory
if [ -z "$PROJECT_DIR" ]; then
  echo "Set PROJECT_DIR with root path of the project";
  exit 1;
fi

echo "$PROJECT_DIR";

# Loading all the required variables
source env.sh;

# To run a Scanner
echo "Number of Scanners to run: 1";
RUNNING_SCANNERS=$(ps -ef | grep start_scanner.py | grep -v grep | wc -l);
if [ "$RUNNING_SCANNERS" -eq 1 ]; then
  echo "One Scanner already running";
else
  echo "Running 1 Scanner";
  nohup python3 "$PROJECT_DIR"/start_scanner.py 2>&1 | tee -a scanner.log &
fi

# To run N ETL
echo "Number of ETLs to run: $NUMBER_OF_ETLS";
RUNNING_ETLS=$(ps -ef | grep start_etl.py | grep -v grep | wc -l);
if [ "$RUNNING_ETLS" -ge "$NUMBER_OF_ETLS" ]; then
  echo "Total Number of ETLs already running: $RUNNING_ETLS".
else
  echo "Running additional $((NUMBER_OF_ETLS-RUNNING_ETLS)) ETLs";
  while [ "$RUNNING_ETLS" -lt "$NUMBER_OF_ETLS" ]; do
    nohup python3 "$PROJECT_DIR"/start_etl.py 2>&1 | tee -a etl.log &
    RUNNING_ETLS=$((RUNNING_ETLS+1))
  done
fi
