"""
Fetches products for multiple search terms from the Open Food Facts API
and inserts them into MongoDB Atlas (nutri_db.open_food_facts by default).

Environment variables used:
  MONGO_URI      - MongoDB connection string (required)
  DB_NAME        - Name of the database (default: "nutri_db")
  COLLECTION_NAME- Name of the collection (default: "open_food_facts")
  PAGE_SIZE      - How many products to fetch per page (default: 50)
  TOTAL_PAGES_PER_SEARCH - How many pages to fetch for each search term (default: 2)
"""

import os
import sys
import time
import logging
import random
import requests
import pymongo

from dotenv import load_dotenv
load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("DB_NAME", "nutri_db")
COLLECTION_NAME = os.getenv("COLLECTION_NAME", "open_food_facts")

PAGE_SIZE = int(os.getenv("PAGE_SIZE", 50))
TOTAL_PAGES_PER_SEARCH = int(os.getenv("TOTAL_PAGES_PER_SEARCH", 2))

search_terms = [
    "chips",
    "chocolate",
    "biscuits",
    "pizza",
    "milk",
    "beer",
    "cereal",
    "cookies",
    "pasta",
    "tea"
]

# Logging setup (INFO level + timestamps)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)

if not MONGO_URI:
    raise ValueError("MONGO_URI environment variable is not set. Cannot connect to MongoDB.")

def fetch_products(search_term, page=1, page_size=50, max_retries=3):
    """
    Fetch a single page of products from the Open Food Facts API for a given search_term.
    Implements a simple retry (max_retries) if requests fail.
    Raises HTTPError if all attempts fail.

    Returns: list of product dicts
    """
    url = "https://world.openfoodfacts.org/cgi/search.pl"
    params = {
        "search_terms": search_term,
        "page": page,
        "page_size": page_size,
        "json": 1
    }

    attempt = 0
    while True:
        try:
            resp = requests.get(url, params=params, timeout=20)
            resp.raise_for_status()
            data = resp.json()
            return data.get("products", [])
        except requests.exceptions.RequestException as e:
            attempt += 1
            if attempt > max_retries:
                logging.error("Max retries exceeded for %s (page=%d). Last error: %s", search_term, page, e)
                raise
            backoff = 2 ** attempt + random.uniform(0, 1)
            logging.warning("Request failed for %s (page=%d), attempt=%d/%d, error=%s. Retrying in %.2fs...",
                            search_term, page, attempt, max_retries, e, backoff)
            time.sleep(backoff)

import logging
from pymongo import UpdateOne

def insert_or_update_mongo(products):
    """
    Upsert a list of product dicts into MongoDB Atlas.
    - If a document with the same 'code' exists, update it.
    - Otherwise, insert a new document.
    """
    if not products:
        return

    client = pymongo.MongoClient(MONGO_URI)
    db = client[DB_NAME]
    coll = db[COLLECTION_NAME]

    operations = []
    for prod in products:
        # Safely convert to string just in case
        code = str(prod.get("code", ""))
        if not code:
            logging.warning("Skipping product with no 'code': %s", prod)
            continue

        # Upsert: if "code" is found, update, else insert
        operations.append(
            UpdateOne(
                {"code": code},
                {"$set": prod},  
                upsert=True
            )
        )

    if not operations:
        logging.info("No valid products to upsert.")
        client.close()
        return

    result = coll.bulk_write(operations)

    logging.info(
        "Upsert operation complete. Matched: %d, Modified: %d, Upserted: %d",
        result.matched_count,
        result.modified_count,
        len(result.upserted_ids)
    )

    client.close()

def main():
    """
    For each search term in `search_terms`, fetch multiple pages
    from the Open Food Facts API, and insert them into MongoDB.
    """
    logging.info("Starting ingestion with search_terms=%s", search_terms)
    logging.info("Using DB=%s, Collection=%s, PAGE_SIZE=%d, TOTAL_PAGES_PER_SEARCH=%d",
                 DB_NAME, COLLECTION_NAME, PAGE_SIZE, TOTAL_PAGES_PER_SEARCH)

    total_inserted_global = 0

    for term in search_terms:
        logging.info("Processing search term: '%s'", term)
        total_inserted_this_term = 0

        for page in range(1, TOTAL_PAGES_PER_SEARCH + 1):
            logging.info("Fetching page %d/%d for term '%s'", page, TOTAL_PAGES_PER_SEARCH, term)

            products = fetch_products(term, page=page, page_size=PAGE_SIZE)
            if not products:
                logging.info("No products returned for '%s' at page %d. Stopping early for this term.",
                             term, page)
                break

            insert_or_update_mongo(products)
            total_inserted_this_term += len(products)

            nap = random.uniform(1, 3)
            logging.info("Fetched %d products for '%s' page %d. Sleeping %.1fs...", len(products), term, page, nap)
            time.sleep(nap)

        logging.info("Done with term '%s', total inserted: %d", term, total_inserted_this_term)
        total_inserted_global += total_inserted_this_term

    logging.info("All search terms processed. Total products inserted across all terms: %d", total_inserted_global)

if __name__ == "__main__":
    main()