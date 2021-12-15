#imports
import os
import tweepy as tw
import pandas as pd
import requests
import datetime
import dateutil.parser
import json
import csv
import time
import re
from textblob import TextBlob
import nltk
#nltk.download(["names","stopwords","state_union","twitter_samples","movie_reviews","averaged_perceptron_tagger","vader_lexicon","punkt"])
from nltk.sentiment import SentimentIntensityAnalyzer


#api keys
consumer_key= '5LZkO4YWu1t2AiO77wbaW7ecq'
consumer_secret= 'zBDdoexpLi5gncsSoWtQTUzTLl2fW0Cgg4vdrkcmDMLYGZVisK'
access_token= '1461496441764040705-nhxnP7sGS3MIg1HTl3glHdOLqLhjfQ'
access_token_secret= '6dOhRvP1CWKKjAcZkykEbaW2N4VrH080wusx3yVjK5BNB'
bear_token = 'AAAAAAAAAAAAAAAAAAAAAJF0WAEAAAAA%2FplYOxS2h6bAKxSs5Rhm%2Fxs8pNc%3DflDeC61h2B0yx4R2uVc04xlGwuT1gdw7cTq4PTNcA5fw84Iivk'

#api authorization
'''
auth = tw.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
api = tw.API(auth, wait_on_rate_limit=True)

searchWords = "Covid" + "-filter:retweets" + "has:geo" 
startDate = "2020-12-1"

tweets = tw.Cursor(api.search_tweets,
                q=searchWords,
                #geocode = "30.6280,96.3344,25mi",
                count = 10,
                lang="en").items()

for tweet in tweets:
    print(tweet.text)
#users_locs = [[tweet.user.screen_name, tweet.user.location] for tweet in tweets]
'''
os.environ['TOKEN'] = bear_token
def auth():
    return os.getenv('TOKEN')

def create_headers(bearer_token):
    headers = {"Authorization": "Bearer {}".format(bearer_token)}
    return headers

def create_url(keyword, start_date, end_date, max_results = 10):
    
    search_url = "https://api.twitter.com/2/tweets/search/all" #Change to the endpoint you want to collect data from

    #change params based on the endpoint you are using
    query_params = {'query': keyword,
                    'start_time': start_date,
                    'end_time': end_date,
                    'max_results': max_results,
                    'expansions': 'author_id,in_reply_to_user_id,geo.place_id',
                    'tweet.fields': 'id,text,author_id,in_reply_to_user_id,geo,conversation_id,created_at,lang,public_metrics,referenced_tweets,reply_settings,source',
                    'user.fields': 'id,name,username,created_at,description,public_metrics,verified',
                    'place.fields': 'full_name,id,country,country_code,geo,name,place_type',
                    'next_token': {}}
    return (search_url, query_params)

def connect_to_endpoint(url, headers, params, next_token = None):
    params['next_token'] = next_token   #params object received from create_url function
    response = requests.request("GET", url, headers = headers, params = params)
    print("Endpoint Response Code: " + str(response.status_code))
    if response.status_code != 200:
        raise Exception(response.status_code, response.text)
    return response.json()

def append_to_csv(json_response, fileName):

    #A counter variable
    counter = 0

    #Open OR create the target CSV file
    csvFile = open(fileName, "a", newline="", encoding='utf-8')
    csvWriter = csv.writer(csvFile)

    #Loop through each tweet
    for tweet in json_response['data']:
        
        # We will create a variable for each since some of the keys might not exist for some tweets
        # So we will account for that

        # 1. Author ID
        author_id = tweet['author_id']

        # 2. Time created
        created_at = dateutil.parser.parse(tweet['created_at'])

        # 3. Geolocation
        if ('place' in tweet):   
            geo = tweet['geo']['place_id']
        else:
            geo = " "

        # 4. Tweet ID
        tweet_id = tweet['id']

        # 5. Language
        lang = tweet['lang']

        # 6. Tweet text
        #cleaning the tweet by removing hashtags/mentions/urls
        cTweet = re.sub("@[A-Za-z0-9_]+","", tweet['text'])
        cTweet = re.sub("#[A-Za-z0-9_]+","", cTweet)
        cTweet = re.sub("([^0-9A-Za-z \t])|(\w+:\/\/\S+)", "", cTweet)
        text = cTweet
        
        # 7. Sentiment
        sentiment = SentimentIntensityAnalyzer()
        sentiment_value = sentiment.polarity_scores(cTweet)["compound"]

        # Assemble all data in a list
        res = [author_id, created_at, geo, tweet_id, lang, text, sentiment_value]
        
        # Append the result to the CSV file
        csvWriter.writerow(res)
        counter += 1

    # When done, close the CSV file
    csvFile.close()

    # Print the number of tweets for this iteration
    print("# of Tweets added from this response: ", counter) 

#for sentient analysis




#Inputs for the request
#point_radius:[96.3344 30.6280 25mi] point_radius:[95.3698 29.7604 25mi]
bearer_token = auth()
headers = create_headers(bearer_token)
keyword = '(TAMU OR tamu OR Texas A&M) (COVID OR covid OR coronavirus OR Coronavirus OR COVID-19 OR covid19 OR Virus OR virus)\
-retweets lang:en has:geo'
start_time = "2019-12-01T00:00:00.000Z"
end_time = "2021-10-01T00:00:00.000Z"
start_list =    ['2019-12-01T00:00:00.000Z',
                 '2020-01-01T00:00:00.000Z',
                 '2020-02-01T00:00:00.000Z',
                 '2020-03-01T00:00:00.000Z',
                 '2020-04-01T00:00:00.000Z',
                 '2020-05-01T00:00:00.000Z',
                 '2020-06-01T00:00:00.000Z',
                 '2020-07-01T00:00:00.000Z',
                 '2020-08-01T00:00:00.000Z',
                 '2020-09-01T00:00:00.000Z',
                 '2020-10-01T00:00:00.000Z',
                 '2020-11-01T00:00:00.000Z',
                 '2020-12-01T00:00:00.000Z',
                 '2021-01-01T00:00:00.000Z',
                 '2021-02-01T00:00:00.000Z',
                 '2021-03-01T00:00:00.000Z',
                 '2021-04-01T00:00:00.000Z',
                 '2021-05-01T00:00:00.000Z',
                 '2021-06-01T00:00:00.000Z',
                 '2021-07-01T00:00:00.000Z',
                 '2021-08-01T00:00:00.000Z',
                 '2021-09-01T00:00:00.000Z',
                 '2021-10-01T00:00:00.000Z']

end_list =      ['2019-12-31T00:00:00.000Z',
                 '2020-01-31T00:00:00.000Z',
                 '2020-02-28T00:00:00.000Z',
                 '2020-03-31T00:00:00.000Z',
                 '2020-04-30T00:00:00.000Z',
                 '2020-05-31T00:00:00.000Z',
                 '2020-06-30T00:00:00.000Z',
                 '2020-07-31T00:00:00.000Z',
                 '2020-08-31T00:00:00.000Z',
                 '2020-09-30T00:00:00.000Z',
                 '2020-10-31T00:00:00.000Z',
                 '2020-11-30T00:00:00.000Z',
                 '2020-12-31T00:00:00.000Z',
                 '2021-01-31T00:00:00.000Z',
                 '2021-02-28T00:00:00.000Z',
                 '2021-03-31T00:00:00.000Z',
                 '2021-04-30T00:00:00.000Z',
                 '2021-05-31T00:00:00.000Z',
                 '2021-06-30T00:00:00.000Z',
                 '2021-07-31T00:00:00.000Z',
                 '2021-08-31T00:00:00.000Z',
                 '2021-09-30T00:00:00.000Z',
                 '2021-10-31T00:00:00.000Z']
max_results = 500

#Total number of tweets we collected from the loop
total_tweets = 0

#creates file
csvFile = open("data4.csv", "a", newline="", encoding='utf-8')
csvWriter = csv.writer(csvFile)
#Create headers for data
csvWriter.writerow(['author id', 'created at', 'geo', 'id','lang','tweet', 'sentiment value'])
csvFile.close()

for i in range(0,len(start_list)):

    # Inputs
    count = 0 # Counting tweets per time period
    max_count = 100 # Max tweets per time period
    flag = True
    next_token = None
    
    # Check if flag is true
    while flag:
        # Check if max_count reached
        if count >= max_count:
            break
        print("-------------------")
        print("Token: ", next_token)
        url = create_url(keyword, start_list[i],end_list[i], max_results)
        json_response = connect_to_endpoint(url[0], headers, url[1], next_token)
        result_count = json_response['meta']['result_count']

        if 'next_token' in json_response['meta']:
            # Save the token to use for next call
            next_token = json_response['meta']['next_token']
            print("Next Token: ", next_token)
            if result_count is not None and result_count > 0 and next_token is not None:
                print("Start Date: ", start_list[i])
                append_to_csv(json_response, "data4.csv")
                count += result_count
                total_tweets += result_count
                print("Total # of Tweets added: ", total_tweets)
                print("-------------------")
                time.sleep(5)                
        # If no next token exists
        else:
            if result_count is not None and result_count > 0:
                print("-------------------")
                print("Start Date: ", start_list[i])
                append_to_csv(json_response, "data4.csv")
                count += result_count
                total_tweets += result_count
                print("Total # of Tweets added: ", total_tweets)
                print("-------------------")
                time.sleep(5)
            
            #Since this is the final request, turn flag to false to move to the next time period.
            flag = False
            next_token = None
        time.sleep(5)
print("Total number of results: ", total_tweets)