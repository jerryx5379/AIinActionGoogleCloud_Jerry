import json
from google.cloud import storage
from pymongo import MongoClient
from dotenv import load_dotenv
import os

load_dotenv()

# --- Configure these ---
GCS_BUCKET_NAME = 'climate-data-user123'

MONGODB_DB_NAME = "Travel_Planner"
MONGODB_COLLECTION_NAME = "Comments"
MONGODB_URI = os.environ.get("MONGODB_URI")
# -----------------------

def download_jsonl_from_gcs(bucket_name, blob_name):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    # Download blob content as string
    content = blob.download_as_text()
    return content

def push_to_mongodb(docs, uri, db_name, collection_name):
    client = MongoClient(uri)
    db = client[db_name]
    collection = db[collection_name]

    # Insert many documents
    result = collection.insert_many(docs)
    print(f"Inserted {len(result.inserted_ids)} documents")

def main():
    with open('locations_temp.txt', 'r') as file:
        locations = [line.strip() for line in file]

    for location in locations:
        GCS_BLOB_NAME = f"comments/{location}.jsonl"
        jsonl_str = download_jsonl_from_gcs(GCS_BUCKET_NAME, GCS_BLOB_NAME)

        docs = []
        for line in jsonl_str.splitlines():
            if line.strip():
                obj = json.loads(line)

                docs.append(obj)

        push_to_mongodb(docs, MONGODB_URI, MONGODB_DB_NAME, MONGODB_COLLECTION_NAME)
        print(f"finished {location}")

if __name__ == "__main__":
    main()
