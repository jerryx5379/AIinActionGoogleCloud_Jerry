from google.cloud import storage
from io import BytesIO
import json
from dotenv import load_dotenv
import os
from google import genai
from google.genai.types import EmbedContentConfig

load_dotenv()
PROJECT_ID = os.environ.get("PROJECT_ID")

client = storage.Client()
bucket = client.bucket("climate-data-user123")

client_gemini = genai.Client(location="global", project=PROJECT_ID, vertexai=True)


def GCS_results_to_list(location):
    prefix = f"batch_results/{location}.jsonl/"
    blobs = list(bucket.list_blobs(prefix=prefix))

    if len(blobs) > 0:
        target_blob = blobs[1]  

        # BytesIO to capture blob content
        buffer = BytesIO()
        target_blob.download_to_file(buffer)
        buffer.seek(0)

        content = buffer.read().decode("utf-8", errors="ignore").splitlines()
        return content

        
    else:
        return 1/0
    
def request_response_text_from_list(jsonthing):
    try:
        line = json.loads(jsonthing)
        request_text = line["request"]["contents"][0]["parts"][0]["text"]
        response_text = line["response"]["candidates"][0]["content"]["parts"][0]["text"]
        response_text = response_text.strip()

        return request_text, response_text
    except:
        return 1/0
    
def get_embedding(text):

    response = client_gemini.models.embed_content(
        model="text-embedding-005",
        contents=text,
        config=EmbedContentConfig(
            task_type="QUESTION_ANSWERING",  
        ),
        )
    embedding = response.embeddings[0].values
    return embedding

if __name__ == "__main__":
    with open('locations_temp.txt', 'r') as file:
        locations = [line.strip() for line in file]

    for location in locations:
        try:
            results_list = GCS_results_to_list(location)
        except:
            print(f"Error retrieving from GCS in {location}")
            continue
        
        # list of dictionaries that will include {"location": str, "text": str, "text_embedding": array}
        list_of_dics = []
        i = 0
        # each element in results_list is a result json to see if the comment was relevant
        for result in results_list:
            # each entry has a location, text, text_embedding
            entry = {}

            # request_text is the original reddit comment
            # response_text is the result from gemini: T or F
            
            
            try:
                request_text, response_text = request_response_text_from_list(result)
                # take off the prompt from request_text
                request_text = request_text.removeprefix("Determine if the comment from reddit contains experiences in Algarve.\nSTART OF TEXT:\n").removesuffix("\nEND OF TEXT\nDo you think that this comment contains experiences in Algarve? Only respond with T or F with no other commentaries or descriptors. Bot comments are F\n")
            except:
                print(f"Trouble extracting from json in {result}")
                continue
            
            # gemini determines this comment is relevant 
            if response_text != 'T':
                continue
            
            # get the vector embedding
            embedding = get_embedding(request_text)

            entry["location"] = location
            entry["text"] = request_text
            entry["text_embedding"] = embedding

            list_of_dics.append(entry)
            i += 1
            print(f"Added an entry! {i}")
            if i > 4000:
                break

        filename = "reddit3.jsonl"
        with open(filename, "w", encoding="utf-8") as f:
            for entry1 in list_of_dics:
                json_line = json.dumps(entry1)
                f.write(json_line + "\n")

        blob_destination = f"comments/{location}.jsonl"
        blob = bucket.blob(blob_destination)
        blob.upload_from_filename(filename)

        print(f"Finished {location}")