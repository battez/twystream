'''stream a set of users current tweets into a database'''

# get the Twitter API app Oauth tokens
import sys
TWITDIR = 'U:\Documents\Project\demoapptwitter'
sys.path.insert(0, TWITDIR)
import config

# Thread as the consumer of the Twitter stream will run in a separate thread.
from threading import Thread
import threading

# provide a lock-free way to move messages from one thread to another.
from queue import Queue

from twython import TwythonStreamer

# handle certain errors that streaming can generate:
from requests.exceptions import ChunkedEncodingError

import json
import random
import time

# basic logging for this task.
import logging
FORMAT = "%(asctime)-15s %(message)s"
logging.basicConfig(filename="log.txt", level=logging.INFO, format=FORMAT)


# implement our extension to Twythons streamer
class TwitterStream(TwythonStreamer):
    '''Set up a queue for the tweets. Provides information on what to do
     when a tweet is received and on errors.'''
    def __init__(self, consumer_key, consumer_secret, token, token_secret, tqueue):
        self.tweet_queue = tqueue
        self._stopevent = threading.Event( )
        # pass credentials to the parent class for authenticating:
        super(TwitterStream, self).__init__(consumer_key, consumer_secret, token, token_secret)

    def on_success(self, data):
        if 'text' in data:
            self.tweet_queue.put(data)

    def on_error(self, status_code, data):
        print(status_code)
        
        # Uncomment to stop trying to get data because of the error
        # self.disconnect()


def stream_tweets(tweets_queue, querystring, filter='follow'):
    ''' samples the stream API and handles errors'''
    
    # OAuth credentials 
    consumer_key = config.ul_consumer_key
    consumer_secret = config.ul_consumer_secret
    token = config.ul_access_token
    token_secret = config.ul_access_secret

    try:
        stream = TwitterStream(consumer_key, consumer_secret, token, token_secret, tweets_queue)
        # filter on users / keywords
        if (filter == 'follow'):
            stream.statuses.filter(follow=querystring)
        elif (filter == 'track'):
            stream.statuses.filter(track=querystring, language='en')
        else:
            stream.statuses.filter(locations=querystring, language='en')

    except ChunkedEncodingError:
        # Sometimes the API sends back one byte less than expected which results in an exception in the
        # current version of the requests library
        stream_tweets(tweet_queue)


def process_tweets(dbc, tweets_queue):
    '''loop over the queue and process each elementwise'''
    count = 0
    max_captured = 1000000
    while True:
    
        if (count < max_captured) :
            tweet = tweets_queue.get()
            count += 1
        else:
            print('max reached, or timer expired - num. collected:' + str(count))
            
            break

        # debug to show it running initially:
        if count < 5:
            print(tweet['text'].encode('utf-8'))
        
        # insert tweet dict into mongodb
        dbc.insert_one(tweet)
        tweets_queue.task_done()

    return

def get_dbc(db_name, collection, host='localhost:27017'):
    '''Convenience wrapper for connecting to a collection 
    in mongoDB'''
    from pymongo import MongoClient
    try:
        client = MongoClient(host)
    except e:
        print ("Could not connect to MongoDB: %s" % e)

    db = client[db_name]

    return db[collection]


if __name__ == '__main__':
    ''' run the script, be sure to adjust the query 
    as needed'''

    logging.info('started')

    # choose which filter and query will be streamed
    filters = ['follow', 'locations', 'track']
    query = ''
    # set the Twitter Stream type of query here:
    filtering = filters[1]

    if(filtering == 'follow'):
        from twython_search_api_lib import load_tweet_ids

        # load user IDs we will track:
        ids = load_tweet_ids(TWITDIR + r'\results.json', 1000)

        # ...and remove any users we do not wish to search on:
        users = [user for user in ids if user not in config.excluded_users]
        # then draw a random subsample of 50 of these:
        random.shuffle(users)
        users = random.sample(users, 50)

        query = ",".join(users)
    
    elif (filtering == 'locations'):
        # FIXME: take the prioritised tweets:
        dbc = get_dbc('Twitter', 'gauges')
        print('sampling...')
        sampled = dbc.aggregate([{'$sample':{'size':25}}])
        prefix = ''
        for document in sampled:
        
            coords = [str(coord) for coord in document['bounding_box']]
            query += prefix + ','.join(coords)
            prefix = ','
           
    else:
        # TODO: keyword tracking if needed.
        pass 

    # Store the Twitter Stream of tweets in a remote db via pymongo
    dbc = get_dbc('database', 'streamedtweets', config.MONGO_URI)

    # set up the queue & htread to make it more robust:
    tweet_queue = Queue()

    # setup a thread whose functionality is the target:
    t = Thread(name='Stream-Tweets', target=stream_tweets, \
        args=(tweet_queue, query, filtering), daemon=True)
    t.start() # since it is a daemon, we don't t.join() .

    # infinite loop that stores the tweets from the queue:
    process_tweets(dbc, tweet_queue)


    
