import pymongo
from dotenv import load_dotenv
import os
from google import genai
from google.genai import types
from google.genai.types import EmbedContentConfig


load_dotenv()
PROJECT_ID = os.environ.get("PROJECT_ID")
MONGODB_URI = os.environ.get("MONGODB_URI")

client_gemini = genai.Client(location="global", project=PROJECT_ID, vertexai=True)


# connect to your Atlas cluster
client = pymongo.MongoClient(MONGODB_URI)
def vector_search(vector, location):
    # different pipeline based on location search or all search
    if location != "all":
        pipeline = [
        {
            '$vectorSearch': {
            'index': 'vector_index', 
            'path': 'text_embedding', 
            'queryVector': vector, 
            'exact': False, 
            'limit': 100,
            'numCandidates': 2000,
            'filter':{
                'location':location
            }
            }
        }, {
            '$project': {
            '_id': 0,
            'text': 1, 
            'score': {
                '$meta': 'vectorSearchScore'
            }
            }
        }
        ]
    else:
        pipeline = [
        {
            '$vectorSearch': {
            'index': 'vector_index', 
            'path': 'text_embedding', 
            'queryVector': vector, 
            'exact': False, 
            'limit': 100,
            'numCandidates': 2000
            }
        }, {
            '$project': {
            '_id': 0,
            'text': 1, 
            'score': {
                '$meta': 'vectorSearchScore'
            }
            }
        }
        ]
    # run pipeline
    result = client["Travel_Planner"]["Comments"].aggregate(pipeline)
    return result

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

def summarize(text):
    input_text = text + "\n\n Respond with a summary of the comments above, focusing on general trends. Only respond with the summary and no other analysis"

    model = "gemini-2.0-flash-001"
    contents = [
        types.Content(
        role="user",
        parts=[
        types.Part.from_text(text=input_text)
        ]
        )
    ]

    generate_content_config = types.GenerateContentConfig(
        temperature = .1,
        top_p = 1,
        max_output_tokens = 8192,
        safety_settings = [types.SafetySetting(
        category="HARM_CATEGORY_HATE_SPEECH",
        threshold="OFF"
        ),types.SafetySetting(
        category="HARM_CATEGORY_DANGEROUS_CONTENT",
        threshold="OFF"
        ),types.SafetySetting(
        category="HARM_CATEGORY_SEXUALLY_EXPLICIT",
        threshold="OFF"
        ),types.SafetySetting(
        category="HARM_CATEGORY_HARASSMENT",
        threshold="OFF"
        )],
    )
    response = client_gemini.models.generate_content(
        model=model,
        contents=contents,
        config=generate_content_config,
    )

    return response.text

def analyze_results(results):
    combined_comments = ""
    index = 1
    for result in results:
        if result['score'] < 0.72:
            continue
        modified_text = f"Comment {index}: " + result['text'] + "\n" 
        combined_comments = combined_comments + modified_text
        index += 1

    summary = summarize(combined_comments)
    return summary

def rate_location(summary, location, prompt):
    input_text = summary + "\n\nEND OF SUMMARY\nThe above text was a summary of comments related to the prompt'" + prompt + f"'. Could you rate {location} from a scale from 0 to 10 given this summary? 3 is poor, 5 is average, and 7 is great. Only include your rating number in your response with no other commentaries or descriptors." 
                

    model = "gemini-2.0-flash-001"
    contents = [
        types.Content(
        role="user",
        parts=[
        types.Part.from_text(text=input_text)
        ]
        )
    ]

    generate_content_config = types.GenerateContentConfig(
        temperature = .1,
        top_p = 1,
        max_output_tokens = 8192,
        safety_settings = [types.SafetySetting(
        category="HARM_CATEGORY_HATE_SPEECH",
        threshold="OFF"
        ),types.SafetySetting(
        category="HARM_CATEGORY_DANGEROUS_CONTENT",
        threshold="OFF"
        ),types.SafetySetting(
        category="HARM_CATEGORY_SEXUALLY_EXPLICIT",
        threshold="OFF"
        ),types.SafetySetting(
        category="HARM_CATEGORY_HARASSMENT",
        threshold="OFF"
        )],
    )
    response = client_gemini.models.generate_content(
        model=model,
        contents=contents,
        config=generate_content_config,
    )

    return response.text

def short_summary(summary):
    inputtext = summary + "\n\nEND OF SUMMARY\nThe text above is a comprehensive summary. Could you response with a shorten version that is just a few sentences. Only respond with the shorten summary with no other commentaries or descriptors."

    model = "gemini-2.0-flash-001"
    contents = [
        types.Content(
        role="user",
        parts=[
        types.Part.from_text(text=inputtext)
        ]
        )
    ]

    generate_content_config = types.GenerateContentConfig(
        temperature = .2,
        top_p = 1,
        max_output_tokens = 8192,
        safety_settings = [types.SafetySetting(
        category="HARM_CATEGORY_HATE_SPEECH",
        threshold="OFF"
        ),types.SafetySetting(
        category="HARM_CATEGORY_DANGEROUS_CONTENT",
        threshold="OFF"
        ),types.SafetySetting(
        category="HARM_CATEGORY_SEXUALLY_EXPLICIT",
        threshold="OFF"
        ),types.SafetySetting(
        category="HARM_CATEGORY_HARASSMENT",
        threshold="OFF"
        )],
    )
    response = client_gemini.models.generate_content(
        model=model,
        contents=contents,
        config=generate_content_config,
    )

    return response.text

def get_score_and_short_summary(text,location):
    embedding = get_embedding(text)
    results = list(vector_search(embedding,location))
    # so far the line below returns the large summary 
    answer = analyze_results(results)
    # rate_location has params summary, location, prompt
    rating = rate_location(answer, location, text)
    rating.strip()
    try:
        rating = int(rating)
    except Exception as e:
        print("Could not get rating")
        rating = 5
    
    shortened_summary = short_summary(answer)


    return {"summary": shortened_summary, "Score": rating}


# given text and location (all = all locations), return relevant sources
if __name__ == "__main__":

    
# Plan on return: {
#       "location1":{"summary":str, "score":int}
#       "location2":{"summary":str, "score":int}
#       "location3":{"summary":str, "score":int}
#   }

    text = "where is a good fmaily trip"
    location = "all"
    
    if location == "all":
        print("running")
        with open("locations.txt", "r", encoding="utf-8") as f:
            lines = [line.strip() for line in f if line.strip()]

        my_dict = {}

        for line in lines:
            my_dict[f"{line}"] = get_score_and_short_summary(text,line)
        print(my_dict)

    else:
        response = get_score_and_short_summary(text,location)


    print(my_dict)

