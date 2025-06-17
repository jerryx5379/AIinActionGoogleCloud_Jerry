import os
from google.api_core.client_options import ClientOptions
from google.cloud import discoveryengine_v1 as discoveryengine
from google.cloud import storage
import time
import json
from dotenv import load_dotenv
import praw
from datetime import datetime, timedelta, timezone
import base64
from google import genai
from google.genai import types
import random

load_dotenv()
REDDIT_CLIENT_ID = os.environ.get("REDDIT_CLIENT_ID")
REDDIT_CLIENT_SECRET = os.environ.get("REDDIT_CLIENT_SECRET")
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")

# Initialize PRAW
reddit = praw.Reddit(
    client_id=REDDIT_CLIENT_ID,
    client_secret= REDDIT_CLIENT_SECRET,
    user_agent="travelplannerapp by u/Fal_Move_3481",
    ratelimit_seconds= 601

)

# Initialize Google Cloud storage
client_GCS = storage.Client()
bucket_name = "climate-data-user123"
bucket = client_GCS.bucket(bucket_name)

# Initialize Gemini api client
client = genai.Client(api_key=os.environ.get("GEMINI_API_KEY"))

# returns the list of urls from a jsonl file in GCS
def json_objects_from_GCS(location):
    blob_name = f"reddit2/{location}.jsonl"
    blob = bucket.blob(blob_name)
    content = blob.download_as_text()

    urls = []

    for line in content.splitlines():
        if not line.strip():
            continue

        try:
            data = json.loads(line)
            url = data.get("url")
            urls.append(url)

        except json.JSONDecodeError:
            print(f"Skipping malformed line: {line.strip()}")

    return urls

# takes a url, then outputs the reddit post id, return none if not there
def extract_post_id_string_split(reddit_url):
    """
    Extracts the post ID from a Reddit URL using string splitting.
    """
    try:
        # Split the URL by '/comments/'
        parts = reddit_url.split('/comments/') 
        if len(parts) > 1:
            # The part after '/comments/' should contain the ID
            id_and_rest = parts[1]
            
            # Now, split by '/' to get the ID as the first element
            id_parts = id_and_rest.split('/')
            
            # The post ID is the first element before any further slashes or query strings
            post_id_with_query = id_parts[0]
            
            # Remove any query parameters (like ?tl=hi-latn)
            post_id = post_id_with_query.split('?')[0]
            return post_id
        else:
            return None  # '/comments/' not found in the URL
    except Exception as e:
        print(f"Error extracting post ID: {e}")
        return None

# takes post.created_utc and returns bool if created within 3 years
def within_three_years(created_date):
    now_utc = datetime.now(timezone.utc)
    three_years_ago = now_utc - timedelta(days=3 * 365) 
    creation_datetime_utc = datetime.fromtimestamp(created_date, tz=timezone.utc)
    return creation_datetime_utc >= three_years_ago

def gemini_relevance_sort(text,location):
    input = f"""Determine if the follow post from reddit might contain discussion of experiences in {location}.
START OF TEXT:
{text}  
END OF TEXT
Do you think that this post might contain discussion of experiences in {location}? Only respond with T or F with no other commentaries or descriptors
"""


    model = "gemini-2.0-flash-lite"
    contents = [
        types.Content(
            role="user",
            parts=[
                types.Part.from_text(text=input),
            ],
        ),
    ]
    generate_content_config = types.GenerateContentConfig(
        response_mime_type="text/plain",
        system_instruction=[
            types.Part.from_text(text="""Only respond with T or F with no other commentaries or descriptors"""),
        ],
    )

    response = client.models.generate_content(
        model=model,
        contents=contents,
        config=generate_content_config,
    )
    return response.text

def smallest_length(main_body, date_created, reddit_post_ID, reddit_comment_ID, text_urls):
    lengths = [len(main_body), len(date_created), len(reddit_post_ID), len(reddit_comment_ID), len(text_urls)]
    return min(lengths)

def limit_num_postids(ids_list):
    max_num = 1400
    if len(ids_list) < max_num:
        return ids_list
    random.shuffle(ids_list)
    return ids_list[:max_num]

if __name__ == "__main__":

    with open('new_locations_edit.txt', 'r') as file:
            locations = [line.strip() for line in file]

    for location in locations:
        print(f"Started {location}")
        # step 1: get list of urls from each jsonl location
        urls = json_objects_from_GCS(location)
        # step 2: turn urls to post_ids: post_id is used to access each post in reddit with PRAW
        post_ids = [extract_post_id_string_split(url) for url in urls]
        post_ids = [item for item in post_ids if item is not None]
        post_ids = limit_num_postids(post_ids)
        # properties of json objects. list form to create jsonl file to then push to GCS in folder "reddit3/"
        
        # location = location
        source_platform = "reddit"
        main_body = []
        date_created = []
        reddit_post_ID = []
        reddit_comment_ID = []
        text_urls = []

        # loop through each post id
        for post_id in post_ids:
            # use PRAW to get the post information
            try:
                post = reddit.submission(id=post_id)
            
                # post has the attributes id, title, selftext, etc
                # step 3: first check if the post is within 3 years
                if not within_three_years(post.created_utc):
                    time.sleep(0.1)
                    continue
                # step 4: then check if the post is relevant with gemini 2.0 flash lite
                # gemini_relevance_sort (text, location) -> bool
                title = post.title
                post_body = post.selftext
                complete_post = title + "\n" + post_body
                
                response = gemini_relevance_sort(complete_post,location)
                if response.strip().upper() != 'T':
                    continue

                # step 5: get all comments in the first three layers of submission and append to temp lists
                #         same for the post
                # main_body | date_created | reddit_post_ID | reddit_comment_ID | text_urls
                main_body.append(complete_post)
                date_created.append(datetime.fromtimestamp(post.created_utc, tz=timezone.utc).isoformat())
                reddit_post_ID.append(post_id)
                reddit_comment_ID.append("n/a")
                text_urls.append(post.permalink)
            
                post.comments.replace_more(limit=0)
                comments = post.comments.list()
                for comment in comments:
                    main_body.append(comment.body)
                    date_created.append(datetime.fromtimestamp(comment.created_utc, tz=timezone.utc).isoformat())
                    reddit_post_ID.append(post_id)
                    reddit_comment_ID.append(comment.id)
                    text_urls.append(comment.permalink)

                print(f"Looked through {post_id}")

            except Exception as e:
                print(f"An error occurred with post {post_id}: {e}")
                index = smallest_length(main_body,date_created, reddit_post_ID, reddit_comment_ID, text_urls)
                main_body = main_body[0:index]
                date_created = date_created[0:index]
                reddit_post_ID = reddit_post_ID[0:index]
                reddit_comment_ID = reddit_comment_ID[0:index]
                text_urls = text_urls[0:index]
                time.sleep(10)

            finally:
                time.sleep(0.1)


        # create a jsonl from the temp list then push to reddit3/ in GCS
        filename = "reddit.jsonl"
        with open(filename, "w", encoding="utf-8", errors='replace') as f:
            for i in range(len(date_created)):
                f.write(json.dumps({
                    "location": location,
                    "source_platform": source_platform,
                    "main_body": main_body[i],
                    "date_created": date_created[i],
                    "reddit_post_ID": reddit_post_ID[i],
                    "reddit_comment_ID": reddit_comment_ID[i],
                    "text_url": text_urls[i],
                }) + "\n")
        
        destination_blob_name = f"reddit3/{location}.jsonl"
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_filename(filename)

        print(f"{location} Finished")

            

                
            



        
