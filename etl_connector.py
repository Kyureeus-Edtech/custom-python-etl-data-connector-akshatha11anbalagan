#!/usr/bin/env python3
"""
ETL connector for CyberGreen Metrics CSV endpoint
Assignment: Kyureeus EdTech – SSN CSE
Done By: Akshatha Anbalagan
3122225001007,CSE A
Date: 2023-10-30
This script extracts data from a CSV endpoint, transforms it, and loads it into MongoDB.
It uses environment variables for configuration, including the CSV URL and MongoDB connection details.
Ftching only 50 rows for testing purposes.Since tbe CSV is large with 1.2M documents.
"""

import os
import sys
import logging
from datetime import datetime, timezone

import pandas as pd
from dotenv import load_dotenv
from pymongo import MongoClient
import requests
from io import StringIO

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("cybergreen_etl")


def load_config():
    load_dotenv()
    return {
        "csv_url": os.getenv("CYBERGREEN_CSV_URL"),
        "mongo_uri": os.getenv("MONGO_URI"),
        "mongo_db": os.getenv("MONGO_DB"),
        "mongo_collection": os.getenv("MONGO_COLLECTION"),
    }


def extract_csv(url, nrows=50):
    print(f"Fetching only {nrows} rows from {url}...")
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        df = pd.read_csv(StringIO(r.text), nrows=nrows)
    return df



def transform(df: pd.DataFrame):
    logger.info("Transforming records")
    df = df.copy()
    df.columns = [c.replace(".", "_") for c in df.columns]
    df["ingestion_ts"] = datetime.now(timezone.utc)
    return df.to_dict(orient="records")


def load_to_mongo(config: dict, records: list):
    logger.info(f"Loading {len(records)} records into MongoDB")
    client = MongoClient(config["mongo_uri"])
    col = client[config["mongo_db"]][config["mongo_collection"]]
    if records:
        col.insert_many(records, ordered=False)
    logger.info("Data load complete")


def main():
    cfg = load_config()
    if not cfg["csv_url"]:
        logger.error("CYBERGREEN_CSV_URL not set in .env")
        sys.exit(1)

    df = extract_csv(cfg["csv_url"], nrows=50) 
    records = transform(df)
    load_to_mongo(cfg, records)
    logger.info("ETL process completed successfully")


if __name__ == "__main__":
    main()
