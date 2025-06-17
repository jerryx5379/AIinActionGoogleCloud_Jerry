import time
from google import genai
from google.genai.types import CreateBatchJobConfig, JobState, HttpOptions
from google.generativeai.types import GenerationConfig 

PROJECT_ID = PROJECT_ID
LOCATION = "us-central1" 

client = genai.Client(project=PROJECT_ID, location=LOCATION, http_options=HttpOptions(api_version="v1"), vertexai=True)

def create_batch_job(location):
    try:
        output_uri = f"gs://climate-data-user123/batch_results/{location}.jsonl" 

        # See the documentation: https://googleapis.github.io/python-genai/genai.html#genai.batches.Batches.create
        job = client.batches.create(
            model="gemini-2.0-flash-lite-001",
            src=f"gs://climate-data-user123/batch_prompts/{location}.jsonl",
            config=CreateBatchJobConfig(
                dest=output_uri,
            ),
        )
    except Exception as e:
        print(f"ERROR at {location}: {e}")

if __name__ == "__main__":
    with open('locations_temp.txt', 'r') as file:
        locations = [line.strip() for line in file]
    for indiv_location in locations:
        create_batch_job(indiv_location)
        print("finished")
        time.sleep(5)

