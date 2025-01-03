import os
import json
import logging
import sys
import pymongo
import boto3

from dotenv import load_dotenv
load_dotenv()

# MongoDB config
MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("DB_NAME")
COLLECTION_NAME = os.getenv("COLLECTION_NAME")

# AWS config
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
S3_BUCKET = os.getenv("S3_BUCKET")

# Logging setup: INFO level + timestamps
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)

def export_mongo_to_s3():
    """
    Connect to MongoDB, fetch all documents from the specified collection,
    convert them to JSON, and upload to S3.
    """
    # 1) Basic validation checks
    if not MONGO_URI:
        raise ValueError("MONGO_URI is not set. Cannot connect to MongoDB.")
    if not AWS_ACCESS_KEY_ID or not AWS_SECRET_ACCESS_KEY or not S3_BUCKET:
        raise ValueError("AWS credentials or S3 bucket name not provided.")

    # 2) Connect to MongoDB
    logging.info("Connecting to MongoDB...")
    client = pymongo.MongoClient(MONGO_URI)
    db = client[DB_NAME]
    coll = db[COLLECTION_NAME]

    # 3) Fetch all docs
    logging.info("Fetching documents from %s.%s", DB_NAME, COLLECTION_NAME)
    cursor = coll.find({})
    data_list = list(cursor)
    doc_count = len(data_list)
    logging.info("Fetched %d documents.", doc_count)

    # 4) Convert to JSON
    logging.info("Converting documents to JSON string...")
    json_data = json.dumps(data_list, default=str)

    # 5) Upload to S3
    logging.info("Connecting to S3 and uploading data...")
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY
    )

    key_name = "openfoodfacts_export.json"
    s3_client.put_object(
        Bucket=S3_BUCKET,
        Key=key_name,
        Body=json_data.encode("utf-8")
    )

    logging.info("Exported %d documents to s3://%s/%s", doc_count, S3_BUCKET, key_name)

    client.close()

def main():
    try:
        export_mongo_to_s3()
    except Exception as e:
        logging.error("Failed to export data to S3. Error: %s", e, exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()