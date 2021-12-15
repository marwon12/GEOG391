import re
import csv
import pandas as pd
from csv import reader
import nltk
from nltk.sentiment import SentimentIntensityAnalyzer

#creates file
csvFile = open("brazosCleanedData.csv", "a", newline="", encoding='utf-8')
csvWriter = csv.writer(csvFile)
#Create headers for data
csvWriter.writerow(['author id', 'created at', 'geo', 'id','lang', 'tweet', 'sentiment value'])


#brazosFile = open("brazos_twitter_data.csv", "a", newline="", encoding='utf-8')
#grimesFile = open("grimes_twitter_data.csv", "a", newline="", encoding='utf-8')
#brazosReader = csv.reader(brazosFile)

df = pd.read_csv("brazos_twitter_data.csv", header=None)
length = len(df.index)
#print(length)
i=0
for i in range(length):
    author_id = df[3][i]
    created_at = df[2][i]
    geo = df[4][i]
    tweet_id = df[0][i]
    language = df[6][i]
    tweet = df[1][i]
    tweet = str(tweet)
    cTweet = re.sub("@[A-Za-z0-9_]+","", tweet)
    cTweet = re.sub("#[A-Za-z0-9_]+","", cTweet)
    cTweet = re.sub("([^0-9A-Za-z \t])|(\w+:\/\/\S+)", "", cTweet)
    sentiment = SentimentIntensityAnalyzer()
    sentiment_value = sentiment.polarity_scores(cTweet)["compound"]
    res = [author_id, created_at, geo, tweet_id, language, cTweet, sentiment_value]
    csvWriter.writerow(res)

csvFile.close()