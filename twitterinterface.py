import concurrent.futures
import toml
import time
import tweepy
import datetime
import sqlite3


config = toml.load("config.toml")
hash_tag_array = config["build"]["HASH_TAG"]
api_key_array = config["build"]["API_KEYS"]

# Data base config

connection = sqlite3.connect("Tweetpy.db")
crsr = connection.cursor()
sql_command = """CREATE TABLE Tweets (  
TweetID INTEGER PRIMARY KEY,  
Handle VARCHAR(100),  
Created_at VARCHAR(30),  
Author_loc VARCHAR(200),  
Text VARCHAR(280));"""

try:
    crsr.execute(sql_command)
except:
    print("table already created, old data may be there.")

min_retweets = str(config["build"]["min_retweets"])
min_fav = str(config["build"]["min_faves"])
start_date = config["build"]["start_date"]

min_retweets = "min_retweets:" + min_retweets
min_fav = "min_faves" + min_fav


def auth(consumer_key, consumer_secret, access_token, access_token_secret, number_in_array):
    try:
        auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
        auth.set_access_token(access_token, access_token_secret)
        api_object = tweepy.API(auth)
        return api_object
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

        if (len(return_array) == 0):
            print("there are currently no valid authentication keys, please update the config.toml")
            exit(1)
        return return_array


def remove_comma(str):
    str = str.replace(",", ".")
    return str


def limit_handle(cursor):
    while 1:
        try:
            yield cursor.next()
        except tweepy.RateLimitError:
            time.sleep(15 * 60)
        except:
            print("Ran out of things to iterate through")
            return


def query_create(hashtag):
    return "#" + hashtag + " " + min_retweets + " " + min_fav


def csv_name(hashtag):
    now = datetime.datetime.now()
    return hashtag + "_" + now.strftime("%Y-%m-%d_%H-%M") + ".csv"


def get_hashtag(auth, hashtag_string):

    for tweet in limit_handle(
            tweepy.Cursor(auth.search, q=hashtag_string, lang="en", since=start_date,
                          tweet_mode="extended").items(10)):

        crsr.execute("insert into Tweets values(?,?,?,?,?)",(tweet.id_str,tweet.author.screen_name,tweet.created_at,tweet.author.location,tweet.full_text))
        print(tweet.author.screen_name)



# + "," + tweet.author.screen_name + "," + tweet.user.id_str + "," + tweet.created_at + "," + tweet.author.location + "," + str(tweet.retweet_count) + "," + str(tweet.favorite_count) + "," + tweet.full_text + "\n"


if __name__ == "__main__":

    authentication_ids = auth_array(api_key_array)

    holding_for_processes = []
    i = 0
    for x in hash_tag_array:
        holding_for_processes.append([x, authentication_ids[i % len(authentication_ids)]])
        i = i + 1

    get_hashtag(authentication_ids[0], "blm")

crsr.execute("SELECT * FROM Tweets")
ans = crsr.fetchall()
print(ans)
connection.commit()


#    with concurrent.futures.ThreadPoolExecutor() as executor:
#        processes = []

 #       for l in holding_for_processes:
 #           processes.append(executor.submit(get_hashtag, l[1], l[0]))

  #      for f in concurrent.futures.as_completed(processes):
  #         print("Completed a process")
