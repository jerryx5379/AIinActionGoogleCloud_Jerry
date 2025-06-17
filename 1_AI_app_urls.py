import os
from google.api_core.client_options import ClientOptions
from google.cloud import discoveryengine_v1 as discoveryengine
from google.cloud import storage
import time
import json


urls = []
client_GCS = storage.Client()
bucket_name = "climate-data-user123"
bucket = client_GCS.bucket(bucket_name)


def location_based_queries(location):
    return  [
        # Personal experiences
        f"{location} travel experience",
        f"what it's like in {location}",
        f"{location} vacation experience",
        f"review of {location}",
        f"First time in {location}",
        # Attractions/ things to do
        f"Top things to do in {location}",
        f"must do in {location}",
        # Budget and cost 
        f"{location} budget travel tips",
        f"How expensive is {location}",
        # Seasonality
        f"Best time to go to {location}",
        f"Weather in {location}",
        # Safety 
        f"Is {location} safe for tourists",
    ]

def search_with_safety_limits(
    project_id: str,
    location: str,
    data_store_id: str,
    search_query: str,
    max_pages: int,
    max_api_calls: int, 
    
) -> None:
    global urls

    """
    Uses Vertex AI Search with both a page limit and a hard API call limit.
    """
    client_options = (
        ClientOptions(api_endpoint=f"{location}-discoveryengine.googleapis.com")
        if location != "global"
        else None
    )
    client = discoveryengine.SearchServiceClient(client_options=client_options)

    serving_config = client.serving_config_path(
        project=project_id,
        location=location,
        data_store=data_store_id,
        serving_config="default_config",
    )

    page_token = None
    page_number = 1
    api_call_counter = 0
    
    while True:
        request = discoveryengine.SearchRequest(
            serving_config=serving_config,
            query=search_query,
            page_size=100,
            page_token=page_token,
        )

        response = client.search(request)
        api_call_counter += 1 

        for result in response.results:
            url = result.document.derived_struct_data.get("link", "No Link Found")
            urls.append(url)


        # 3. Updated exit logic with the new emergency stop condition
        # The loop will break if ANY of these conditions are true.
        stop_reason = None
        if not response.next_page_token:
            stop_reason = "No more results found."
        elif page_number >= max_pages:
            stop_reason = f"Programmed page limit ({max_pages}) reached."
        elif api_call_counter >= max_api_calls:
            stop_reason = f"EMERGENCY STOP: API call limit ({max_api_calls}) reached."

        if stop_reason:
            print(f"\nStopping search. Reason: {stop_reason}")
            break

        # If we are continuing, prepare for the next loop
        page_token = response.next_page_token
        page_number += 1




# we have urls and publish_times
def create_jsonl_to_GCS(location):
    global urls

    filename = "reddit.jsonl"
    with open(filename, "w", encoding="utf-8") as f:
        for i in range(len(urls)):
            f.write(json.dumps({
                "url": urls[i],
            }) + "\n")

    destination_blob_name = f"reddit1/{location}.jsonl"
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(filename)

    urls = []
    

if __name__ == "__main__":
    PROJECT_ID = PROJECT_ID
    LOCATION = "global"
    DATA_STORE_ID = DATA_STORE
    
    # Normal program logic: stop after this many pages.
    MAX_PAGES_TO_FETCH = 10
    # Hard safety net: stop if we ever exceed this many API calls.
    MAX_API_CALLS = 11


    with open('new_locations.txt', 'r') as file:
        locations = [line.strip() for line in file]

    for location in locations:
        queries = location_based_queries(location)

        for query in queries:

            if not query:
                print("Search query cannot be empty.")
            else:
                search_with_safety_limits(
                    PROJECT_ID,
                    LOCATION,
                    DATA_STORE_ID,
                    query,
                    MAX_PAGES_TO_FETCH,
                    MAX_API_CALLS
                )

        
        time.sleep(0.5)
        create_jsonl_to_GCS(location)
        print(f"{location} completed")

    print("All locations completed")

