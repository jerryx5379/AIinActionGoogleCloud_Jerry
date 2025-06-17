Code used to develop "Find your Vacation" for the AI in action google cloud and mongodb hackathon.

Process: (numbering corresponds to numbering in python files)
0. Get 100 popular but diverse vacation locations around the world. -> locations.txt 
1. Create a google cloud AI applications app that uses www.reddit.com/* as a data store. Search the top 100 results based on 12 queries for each location (e.g. Personal experience in {location}, Safety in {location}) Store the result's url in google cloud storage (one folder for each location)
2. Remove duplicates from the the list of urls for each location
3. (Using reddit api wrapper PRAW) Use praw to get the urls's post and comments. Leave out comments older than 3 years. Run a gemini custom prompt to determine if the post is relevant. Is: expand its comments. Isn't: skip
4. Prepare the relevant comments to be batched. String manipulation before and after comment to make it a prompt. This batch operation determines if a comment is relevant or not.
5. Batch the prompts
6. Get results from batch job. Relevant: create json object (later transferred to mongodb) with fields location, text, text_embedding. Ignore irrelevant
7. Transfer json objects from previous steps to mongodb
8. The user from the website makes two inputs: text and location. The text is the prompt and location is the location for that prompt. If location = "all" then loop through all locations.
   a. use input and prompt to get top 100 relevant comments for a location. Use gemini to get a detailed summary of these comments. Create an index of how positively related is the location to the prompt that is an       int. Create a short summary from the detailed summary. Return the short summary and index.

testingnextjs - Copy: this is the next js app that includes the frontend and calls the url in the previous step. 

