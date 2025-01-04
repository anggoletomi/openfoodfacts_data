import os
import sys
import re
import logging
import snowflake.connector
from datetime import datetime

from dotenv import load_dotenv
load_dotenv()

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

# Environment / Config
SNOWFLAKE_ACCOUNT          = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_USER             = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD         = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ROLE             = os.getenv("SNOWFLAKE_ROLE")
SNOWFLAKE_WAREHOUSE_NAME   = os.getenv("SNOWFLAKE_WAREHOUSE_NAME")
SNOWFLAKE_DATABASE_NAME    = os.getenv("SNOWFLAKE_DATABASE_NAME")
SNOWFLAKE_SCHEMA_NAME      = os.getenv("SNOWFLAKE_SCHEMA_NAME")

AWS_ACCESS_KEY_ID          = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY      = os.getenv("AWS_SECRET_ACCESS_KEY")
S3_BUCKET                  = os.getenv("S3_BUCKET")

STAGE_NAME                 = "STAGE_OPENFOODFACTS"
TABLE_NAME                 = "RAW_OPENFOODFACTS"
FILE_PATTERN               = r"openfoodfacts_export_\d{8}_\d{6}\.json"

# Run Query Function
def run_query(conn, sql):
    with conn.cursor() as cur:
        cur.execute(sql)

# Run Query - Fetch All Function
def run_query_fetchall(conn, sql):
    with conn.cursor() as cur:
        cur.execute(sql)
        return cur.fetchall()

# Main
def main():
    # 1) Validate env
    if not SNOWFLAKE_ACCOUNT or not SNOWFLAKE_USER or not SNOWFLAKE_PASSWORD:
        logging.error("Missing Snowflake credentials. Check environment variables.")
        sys.exit(1)
    if not AWS_ACCESS_KEY_ID or not AWS_SECRET_ACCESS_KEY or not S3_BUCKET:
        logging.error("Missing S3 credentials or bucket name. Check environment variables.")
        sys.exit(1)

    # 2) Connect to Snowflake
    try:
        conn = snowflake.connector.connect(
            account=SNOWFLAKE_ACCOUNT,
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            role=SNOWFLAKE_ROLE,
        )
    except Exception as e:
        logging.error("Failed to connect to Snowflake: %s", e)
        sys.exit(1)
    logging.info("Snowflake connection established.")

    try:
        # 3) Create Warehouse, DB, Schema
        run_query(conn, f"""
            CREATE WAREHOUSE IF NOT EXISTS {SNOWFLAKE_WAREHOUSE_NAME}
            WAREHOUSE_SIZE = 'XSMALL'
            AUTO_SUSPEND = 10
            AUTO_RESUME = TRUE
            INITIALLY_SUSPENDED = TRUE;
        """)
        run_query(conn, f"CREATE DATABASE IF NOT EXISTS {SNOWFLAKE_DATABASE_NAME};")
        run_query(conn, f"CREATE SCHEMA IF NOT EXISTS {SNOWFLAKE_DATABASE_NAME}.{SNOWFLAKE_SCHEMA_NAME};")

        # 4) Use them
        run_query(conn, f"USE WAREHOUSE {SNOWFLAKE_WAREHOUSE_NAME};")
        run_query(conn, f"USE DATABASE {SNOWFLAKE_DATABASE_NAME};")
        run_query(conn, f"USE SCHEMA {SNOWFLAKE_SCHEMA_NAME};")

        # 5) Create table if not exists (VARIANT column for JSON)
        run_query(conn, f"""
            CREATE TABLE IF NOT EXISTS {TABLE_NAME} (RAW VARIANT);
        """)

        run_query(conn, f"TRUNCATE TABLE {TABLE_NAME};")
        logging.info("Truncated old data, only new file will remain in table.")

        # 6) Create stage if not exists
        run_query(conn, f"""
            CREATE STAGE IF NOT EXISTS {STAGE_NAME}
            URL = 's3://{S3_BUCKET}/'
            CREDENTIALS = (
                aws_key_id='{AWS_ACCESS_KEY_ID}',
                aws_secret_key='{AWS_SECRET_ACCESS_KEY}'
            )
            FILE_FORMAT = (TYPE = JSON STRIP_OUTER_ARRAY = TRUE);
        """)

        # 7) List stage files, find the LATEST JSON
        rows = run_query_fetchall(conn, f"LIST @{STAGE_NAME}")
        matched_files = []
        for row in rows:
            full_path = row[0]
            short_name = full_path.split(';')[0].split('/')[-1]
            if re.match(FILE_PATTERN, short_name):
                matched_files.append(short_name)

        if not matched_files:
            logging.warning("No matching files found for pattern '%s' in stage '%s'.", FILE_PATTERN, STAGE_NAME)
            sys.exit("No files to copy. Exiting.")

        # Sort by timestamp, pick newest
        def parse_ts(fname):
            datepart = fname.replace('openfoodfacts_export_', '').replace('.json', '')
            return datetime.strptime(datepart, '%Y%m%d_%H%M%S')

        matched_files.sort(key=parse_ts)
        latest_file = matched_files[-1]
        logging.info("Latest file determined: %s", latest_file)

        # 8) COPY into table
        copy_sql = f"""
            COPY INTO {TABLE_NAME}
            FROM @{STAGE_NAME}/{latest_file}
            ON_ERROR='ABORT_STATEMENT'
        """
        run_query(conn, copy_sql)
        logging.info("Copied file '%s' into table '%s'.", latest_file, TABLE_NAME)

        # 9) Validate row count
        result = run_query_fetchall(conn, f"SELECT COUNT(*) FROM {TABLE_NAME}")
        final_count = result[0][0]
        logging.info("Row count in %s: %d", TABLE_NAME, final_count)

        conn.close()
        logging.info("Snowflake setup & load complete. Total of %d documents.", final_count)

    except Exception as e:
        logging.error("Unexpected error: %s", e)
        sys.exit(1)

if __name__ == "__main__":
    main()