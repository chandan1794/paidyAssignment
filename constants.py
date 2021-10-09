import pytz
from datetime import datetime

# Timezone
TZ = pytz.UTC

# Minimum date for the project start
MINIMUM_TIME = datetime(2021, 10, 9, tzinfo=TZ)

# File Separator to use for joining and separating multiple
# file paths for ETL jobs
# example: "sample/file/1" join "sample/file/2"
# after joining: "sample/file/1,sample/file/2"
MULTI_FILE_PATH_SEPARATOR = ","
