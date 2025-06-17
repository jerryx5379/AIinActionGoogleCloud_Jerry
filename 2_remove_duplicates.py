import json
from google.cloud import storage

def remove_duplicates(bucket_name, source_blob_name, destination_blob_name):
    """
    Removes duplicates from a SMALL JSONL file by loading it into memory.
    
    Args:
        bucket_name (str): The name of your GCS bucket.
        source_blob_name (str): The path to the source JSONL file.
        destination_blob_name (str): The path for the new unique JSONL file.
    """
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    # 1. Download the entire file's content into memory
    source_blob = bucket.blob(source_blob_name)
    content = source_blob.download_as_text()

    # 2. Process the content
    seen_urls = set()
    unique_lines = []
    
    for line in content.splitlines():
        if not line.strip():
            continue
            
        try:
            data = json.loads(line)
            url = data.get("url")

            # If the URL is new, keep the line
            if url and url not in seen_urls:
                seen_urls.add(url)
                unique_lines.append(line)
        
        except json.JSONDecodeError:
            print(f"Skipping malformed line: {line.strip()}")

    # 3. Upload the result back to GCS
    destination_blob = bucket.blob(destination_blob_name)
    # Join the unique lines back together with newlines
    new_content = "\n".join(unique_lines)
    destination_blob.upload_from_string(new_content, content_type="application/x-jsonlines")
    
    print(f"Successfully removed duplicates. Unique data saved to 'gs://{bucket_name}/{destination_blob_name}'")


# --- Example Usage ---
# Replace with your bucket and file names
GCS_BUCKET_NAME = "climate-data-user123"

with open('new_locations.txt', 'r') as file:
    locations = [line.strip() for line in file]

for location in locations:
    source_file_path = f"reddit1/{location}.jsonl"
    destination_file_path = f"reddit2/{location}.jsonl"
    remove_duplicates(GCS_BUCKET_NAME, source_file_path, destination_file_path)

