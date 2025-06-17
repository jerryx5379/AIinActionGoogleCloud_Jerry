import json
from google.cloud import storage
from dotenv import load_dotenv
import os
import time

client_GCS = storage.Client()
bucket_name = "climate-data-user123"
bucket = client_GCS.bucket(bucket_name)

load_dotenv()
REDDIT_CLIENT_ID = os.environ.get("REDDIT_CLIENT_ID")
REDDIT_CLIENT_SECRET = os.environ.get("REDDIT_CLIENT_SECRET")
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")


def json_objects_from_GCS(location):
    blob_name = f"reddit3/{location}.jsonl"
    blob = bucket.blob(blob_name)
    content = blob.download_as_text()

    urls = []

    for line in content.splitlines():
        if not line.strip():
            continue

        try:
            data = json.loads(line)
            url = data.get("main_body")
            urls.append(url)

        except json.JSONDecodeError:
            print(f"Skipping malformed line: {line.strip()}")

    return urls

def create_and_import_prompts_jsonl(location):
    main_body_list = json_objects_from_GCS(location)
    filename = "reddit1.jsonl"
    with open(filename, "w", encoding="utf-8", errors='replace') as f:
        for main_body in main_body_list:
            prompt = f"""Determine if the comment from reddit contains experiences in {location}.
START OF TEXT:
{main_body}
END OF TEXT
Do you think that this comment contains experiences in {location}? Only respond with T or F with no other commentaries or descriptors. Bot comments are F
"""

            f.write(json.dumps({
                "request": { 
                "contents": [
                    {
                        "role": "user",
                        "parts": [
                            {"text": prompt}
                        ]
                    }
                ]
            }
            }) + "\n")
    
    destination_blob_name = f"batch_prompts/{location}.jsonl"
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(filename)

if __name__ == "__main__":
    with open('locations_temp.txt', 'r') as file:
        locations = [line.strip() for line in file]

    for indiv_location in locations:
        create_and_import_prompts_jsonl(indiv_location)
        time.sleep(1)
        print(f"Finished {indiv_location}")



    

   


