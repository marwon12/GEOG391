import requests
import os
import json
import time
import pandas as pd
from io import StringIO


#API access keys
# consume_key = "J0Kwm6zBMQrO5iOX7dNtbgCjr"
# consume_key_secret = "xvQlhLIEfBbf7uhHkZ5TyJCzpoFZEaDcpIQmBuTnHKmnZ9FAiT"
# access_token = "450327555-IANlKvCE1WebcDcwbOPRRQqQWz8pd4O57QoDPajX"
# access_token_secret = "rcGWZdMoyQ5GpvZSjDook5HmLU3NMopEevtebvlyiEajR"
bear_token = "AAAAAAAAAAAAAAAAAAAAAAymUgEAAAAAmoKq0Mn1urEFinVFXh%2FrC6KEWpA%3DTMVdY934BqckxwobvLfKpJxItCnT4L6Ay5baWUT5opOaKvfzhc"
#!!!!! your bear_token is wrong!

#authentification for API Access
# auth = tweepy.OAuthHandler(consume_key, consume_key_secret)
# auth.set_access_token(access_token, access_token_secret)
# api = tweepy.API(auth)

#for tweet in tweepy.Cursor(api.search_tweets, q='covid').items(20):
    #print(tweet.text)
# filename = 'twitter_data'+(datetime.datetime.now().strftime("%Y-%m-%d-%H"))+'.csv'
# with open (filename, 'a+', newline='') as csvFile:
#     csvWriter = csv.writer(csvFile)
#     for tweet in tweepy.Cursor(api.search_tweets, q='covid', lang = 'en', count=1000).items():
#         tweets_encoded = tweet.text.encode('utf-8')
#         tweets_decoded = tweets_encoded.decode('utf-8')
#         csvWriter.writerow([datetime.datetime.now().strftime("%Y-%m-%d  %H:%M"), tweet.id, tweets_decoded, tweet.created_at, tweet.geo, tweet.place.name if tweet.place else None, tweet.coordinates, tweet._json["user"]["location"]])

search_url = "https://api.twitter.com/2/tweets/search/all"
query = "-is:retweet has:geo lang:en \
(bounding_box:[-95.884629 28.82456588 -95.522629 29.18656588] OR \
bounding_box:[-95.524629 28.82456588 -95.162629 29.18656588] OR \
bounding_box:[-94.444629 29.54456588 -94.082629 29.90656588])"
##!!!!!!!!
## !!!!need to change the bounding_box to your interesting area

max_results = '500'
start_time = '2021-01-01T00:00:00Z'
end_time = '2021-10-01T00:00:00Z'  #the query doesn't include the end_time, so if your real end time is 2021-09-30, then you need to type 2021-10-01 here
tweetFields = 'id,created_at,text,geo,lang,author_id'
placeFields = 'id,name,place_type,geo'
userFields = 'id,location'
expansions = 'geo.place_id,author_id'

query_params = {'query': query,\
                'max_results': max_results,\
                'start_time': start_time,\
                'end_time': end_time,\
                'tweet.fields': tweetFields,\
                'place.fields': placeFields,\
                'user.fields': userFields,\
                'expansions': expansions
                }
def bearer_oauth(r):
    r.headers["Authorization"] = f"Bearer {bear_token}"
    r.headers["User-Agent"] = "v2FullArchiveSearchPython"
    return r

def connect_to_endpoint(url, params):
    # Call api and handling connection error:
    try:
        response = requests.get(url, auth=bearer_oauth, params=params)
    except Exception as e:
        print(e)
        time.sleep(10)
        connect_to_endpoint(url, params)
    # Process exception:
    print("Status Code: " + str(response.status_code))
    if response.status_code == 200:
        return response.json()
    elif response.status_code == 429:
        print("Rate Limit Exceeded. Start waiting for 5 minutes")
        time.sleep(300)
        return connect_to_endpoint(url, params)
    else:
        print(response.status_code, response.text)
        return {}

def jsonTolist(json_response,TweetQueryResultList,UserQueryResultList,placeQueryResultList): #
    for line in json_response['data']:
        TweetQueryResultList[0].append(line['id'])
        TweetQueryResultList[1].append(line['text'])
        TweetQueryResultList[2].append(line['created_at'])
        TweetQueryResultList[3].append(line['author_id'])
        if 'geo' in line:
            if 'place_id' in line['geo']:
                TweetQueryResultList[4].append(line['geo']['place_id'])
            else:
                TweetQueryResultList[4].append(None)
            if 'coordinates' in line['geo']:
                TweetQueryResultList[5].append(line['geo']['coordinates']['coordinates'])
            else:
                TweetQueryResultList[5].append(None)
        else:
            TweetQueryResultList[4].append(None)
            TweetQueryResultList[5].append(None)
        if 'lang' in line:
            TweetQueryResultList[6].append(line['lang'])
        else:
            TweetQueryResultList[6].append(None)
    for exline_user in json_response['includes']['users']:
        UserQueryResultList[0].append(exline_user['id'])
        if 'location' in exline_user:
            UserQueryResultList[1].append(exline_user['location'])
        else:
            UserQueryResultList[1].append(None)
    for eeeexline_geo in json_response['includes']['places']:
        placeQueryResultList[0].append(eeeexline_geo['full_name'])
        if 'bbox' in eeeexline_geo['geo']:
            placeQueryResultList[1].append(eeeexline_geo['geo']['bbox'])
        else:
            placeQueryResultList[1].append(None)
        placeQueryResultList[2].append(eeeexline_geo['name'])
        placeQueryResultList[3].append(eeeexline_geo['place_type'])
        placeQueryResultList[4].append(eeeexline_geo['id'])
    if 'next_token' in json_response['meta']:
        next_token = json_response['meta']['next_token']
    else:
        next_token = 'None'
    return next_token

def listToCSV(List,filename):
    data = pd.DataFrame(List)
    data.to_csv(filename, encoding="utf-8", index=False, header=False)
    ##!!!!!!!!
    #!!!!! you need to deal with the text formatting issues here.

def main():
    tweetcsvfile = r'C:\Users\Esther\Desktop\test\tweet.csv'
    usercsvfile = r'C:\Users\Esther\Desktop\test\user.csv'
    placecsvfile = r'C:\Users\Esther\Desktop\test\place.csv'
    json_response = connect_to_endpoint(search_url, query_params)
    TweetQueryResultList = [[] for i in range(7)]
    UserQueryResultList = [[] for i in range(2)]
    placeQueryResultList = [[] for i in range(5)]
    next_token = jsonTolist(json_response, TweetQueryResultList, UserQueryResultList, placeQueryResultList)  #
    listToCSV(zip(*TweetQueryResultList),tweetcsvfile)
    listToCSV(zip(*UserQueryResultList), usercsvfile)
    listToCSV(zip(*placeQueryResultList), placecsvfile)

    while next_token != 'None':
        query_params_next = {'query': query,\
                'max_results': max_results,\
                'start_time': start_time,\
                'end_time': end_time,\
                'tweet.fields': tweetFields,\
                'place.fields': placeFields,\
                'user.fields': userFields,\
                'expansions': expansions,
                'next_token': next_token
                }
        time.sleep(3)
        json_response = connect_to_endpoint(search_url, query_params_next)
        next_token = jsonTolist(json_response, TweetQueryResultList, UserQueryResultList, placeQueryResultList)  #
        listToCSV(zip(*TweetQueryResultList), tweetcsvfile)
        listToCSV(zip(*UserQueryResultList), usercsvfile)
        listToCSV(zip(*placeQueryResultList), placecsvfile)
        TweetQueryResultList = [[] for i in range(7)]
        UserQueryResultList = [[] for i in range(2)]
        placeQueryResultList = [[] for i in range(5)]

    print('Finished!')

if __name__ == "__main__":
    main()