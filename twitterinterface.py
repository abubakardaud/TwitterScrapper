import concurrent.futures
import json
import sys
import toml
import time
import tweepy
import datetime
import sqlite3
import traceback
from tweepy import RateLimitError, TweepError
import tweepy
import multiprocessing
from textblob import TextBlob


# Globals
counter_var = 0


# Load Config

config = toml.load("config.toml")
hash_tag_array = config["build"]["HASH_TAG"]
api_key_array = config["build"]["API_KEYS"]
ignore_retweet = config["build"]["ignore_retweets"]
if (ignore_retweet == 1):
    ignore_retweet = True
else:
    ignore_retweet = False


notable_min_followers = config["build"]["notable_min_followers"]
start_date = config["build"]["start_date"]
num_requested = config["build"]["num_requested"]

# Intro Script

print("TweetScraper V1.0, Current loaded queries: ")
print(hash_tag_array)

# Data base config

print("connecting to database . . . ")
connection = sqlite3.connect("Tweetpy.db")
crsr = connection.cursor()
sql_command = """CREATE TABLE Tweets (  
TweetID INTEGER PRIMARY KEY,  
Handle VARCHAR(100),  
Created_at VARCHAR(30),  
Author_loc VARCHAR(200),  
Text VARCHAR(280),
Fav_count INTEGER,
created_time VARCHAR(120),
Author_id INTEGER,
sentiment INTEGER);"""

sql_command2 = """CREATE TABLE Authors (  
AuthorID INTEGER PRIMARY KEY,  
Name VARCHAR(200),  
Screen_name VARCHAR(200),
description VARCHAR(400),  
Author_loc VARCHAR(300),  
verified INTEGER,
followers_count INTEGER,
friends_count INTEGER,
favourites_count INTEGER,
statuses_count INTEGER,
default_profile VARCHAR(20),
default_profile_image VARCHAR (20),
created_at VARCHAR(300),
avg_sentiment INTEGER);"""

try:
    crsr.execute(sql_command)
except:
    print("Found Old Tweet Database, data may need to be updated.")

try:
    crsr.execute(sql_command2)
except:
    print("Found Old Author Database, data may need to be updated.")

connection.commit()
print("Database connection successful")


def auth(consumer_key, consumer_secret, access_token, access_token_secret, number_in_array):
    try:
        auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
        auth.set_access_token(access_token, access_token_secret)

        return auth
    except:
        print("authentication failed for number" + str(number_in_array))
        inputedval = input("would you like to terminate the entire program? (Y/N)")
        inputedval.lower()
        if (inputedval == "y") or (inputedval == "yes"):
            exit(1)
        return None


def auth_array(input_array):
    return_array = []
    i = 0
    for key in input_array:
        i = i + 1
        auth_key = auth(key[0], key[1], key[2], key[3], i)
        if (auth_key != None):
            return_array.append(auth_key)

        if len(return_array) == 0:
            print("there are currently no valid authentication keys, please update the config.toml")
            exit(1)
        print("All Auth keys successfully loaded")
        return return_array


def limit_handle(cursor):
    Error_count = 0
    while 1:
        try:
            yield cursor.next()
        except TweepError:
            time.sleep(15 * 60)
            Error_count = Error_count + 1
            if Error_count == 3:
                print("\nMax Error Count reached, terminating cycle")
                return
        except KeyboardInterrupt:
            print("User Keyboard Interrupt")
            print("Cycle ending")
            print("\n")
            return
        except StopIteration:
            print("\n\nCycle completed successfully . . . \n")
            return

        except:
            traceback.print_exc()
            print("\n Terminating cycle")
            return


def get_hashtag(tupple):
    auth = tupple[0]
    auth = tweepy.API(auth)
    hashtag_string = tupple[1]
    global counter_var
    Tweet_fail = 0
    val = None
    print(hashtag_string)
    for tweet in limit_handle(
            tweepy.Cursor(auth.search, q=hashtag_string, lang="en", since=start_date,
                          tweet_mode="extended").items(num_requested)):
        try:
            crsr.execute("insert into Tweets values(?,?,?,?,?,?,?,?,?,?)", (
            tweet.id, tweet.author.screen_name, tweet.created_at, tweet.author.location, tweet.full_text,
            tweet.favorite_count, tweet.created_at, tweet.user.id, val))
        except:
            Tweet_fail = Tweet_fail + 1

        try:
            crsr.execute("insert into Authors values(?,?,?,?,?,?,?,?,?,?,?,?,?,?)", (
                tweet.user.id, tweet.user.name, tweet.user.screen_name, tweet.user.description, tweet.user.location,
                tweet.user.verified, tweet.user.followers_count, tweet.user.friends_count, tweet.user.favourites_count,
                tweet.user.statuses_count, tweet.user.default_profile, tweet.user.default_profile_image,
                tweet.user.created_at, val))
        except:
            Tweet_fail = Tweet_fail + 1

        counter_var = counter_var + 1
        sys.stdout.write("\rNum Tweets Counted = %i" % counter_var)
        sys.stdout.flush()

        if counter_var % 100 == 1:
            connection.commit()
    connection.commit()


#Streaming code,

class StreamListener(tweepy.StreamListener):
    def on_status(self, status):
        print(status.text)
        if status.retweeted_status:
            if ignore_retweet:
                return

    def on_error(self, status_code):
        print(status_code)
        if status_code == 420:
            time.sleep(15 * 60 )

    def on_data(self, raw_data):
        tweet = json.loads(raw_data)
        print (tweet)
        if 'text' in tweet:

                crsr.execute("insert into Authors values(?,?,?,?,?,?,?,?,?,?,?,?,?,?)", (
                    tweet['user']['id'], tweet['user']['name'], tweet['user']['screen_name'], tweet['user']['description'], tweet['user']['location'],
                    tweet['user']['verified'], tweet['user']['followers_count'], tweet['user']['friends_count'],
                    tweet['user']['favourites_count'],
                    tweet['user']['statuses_count'], tweet['user']['default_profile'], tweet['user']['default_profile_image'],
                    tweet['user']['created_at'], None))

                crsr.execute("insert into Tweets values(?,?,?,?,?,?,?,?,?)", (
                    tweet['id'], tweet['user']['screen_name'], tweet['created_at'], tweet['user']['location'], tweet['text'],
                    tweet['favorite_count'], tweet['created_at'], tweet['user']['id'], None))
                connection.commit()
                return True


def run_streaming(auth):
    stream_listener = StreamListener()
    stream = tweepy.Stream(auth=auth, listener=stream_listener)
    stream.filter(track=hash_tag_array,languages=['en'],stall_warnings= True)


# end of streaming code.


if __name__ == "__main__":



    authentication_ids = auth_array(api_key_array)


    '''
    pool_array = []
    counter = 0
    for i in hash_tag_array:
        Holding_Tupple = [authentication_ids[counter % len(authentication_ids)], i]
        pool_array.append(Holding_Tupple)
        counter = counter + 1

    pool = multiprocessing.Pool()
    results = pool.map(get_hashtag,pool_array)
    '''
    print(authentication_ids)
    run_streaming(authentication_ids[0])
    #get_hashtag([authentication_ids[0],hash_tag_array[0]])
    connection.commit()

