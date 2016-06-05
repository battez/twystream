'''stream a set of users current tweets into a database'''
# Thread as the consumer of the Twitter stream will run in a separate thread.
from threading import Thread

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
import sys
CURR_PLATFORM = sys.platform
if CURR_PLATFORM != 'linux':
    TWITDIR = 'U:\Documents\Project\demoapptwitter'
    SCRAPEDIR = 'U:\Documents\Project\scrape'
else:
    TWITDIR = '/home/luke/programming/projectfiles/demoapptwitter' # FIXME:
    SCRAPEDIR = '/home/luke/programming/scraping'#FIXME:



# get the Twitter API app Oauth tokens
sys.path.insert(0, TWITDIR)
import config

# get fn updating water levels:

sys.path.insert(0, SCRAPEDIR)
import parse_xml_locs, retrieve_latest_from_csv

# implement our extension to Twythons streamer
class TwitterStream(TwythonStreamer):
    '''Set up a queue for the tweets. Provides information on what to do
     when a tweet is received and on errors.'''
    def __init__(self, consumer_key, consumer_secret, token, token_secret, \
        tqueue):
        self.tweet_queue = tqueue
        # FIXME: use in case of threading
        # self._stopevent = threading.Event( )

        # pass credentials to the parent class for authenticating:
        super(TwitterStream, self).__init__(consumer_key, consumer_secret, \
            token, token_secret)

    def on_success(self, data):
        if 'text' in data:
            self.tweet_queue.put(data)

    def on_error(self, status_code, data):
        logging.warning('code: ' + status_code)
        
        # Uncomment to stop trying to get data because of the error
        # self.disconnect()


def stream_tweets(tweets_queue, querystring, filter='follow'):
    ''' samples the stream API and handles errors'''
    print(querystring)
    # OAuth credentials 
    consumer_key = config.ul_consumer_key
    consumer_secret = config.ul_consumer_secret
    token = config.ul_access_token
    token_secret = config.ul_access_secret

    try:
        stream = TwitterStream(consumer_key, consumer_secret, token, \
            token_secret, tweets_queue)
        # filter on users / keywords
        if (filter == 'follow'):
            stream.statuses.filter(follow=querystring)
        elif (filter == 'track'):
            stream.statuses.filter(track=querystring, language='en')
        else:
            stream.statuses.filter(locations=querystring, language='en')

    except ChunkedEncodingError:
        # Sometimes the API sends back one byte less than expected which 
        # results in an exception in the
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
            print('max reached, or timer expired; num. collected:' + str(count))
            
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
    logging.info('Task started')

    # choose which filter and query will be streamed
    filters = ['follow', 'locations', 'track', 'locfrance']
    query = ''

    # set the Twitter Stream type of query here:
    if CURR_PLATFORM != 'linux':
        filtering = filters[1]
    else:
        filtering = filters[3] 

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


        dbc = get_dbc('Twitter', 'gauges')
        
        print('processing gauge levels...')
        # get all gauges averages:
        gauges = dbc.find({}, {'loc_id':1,'avg_level':1, '_id':0})
        
        # retrieve the location IDs of all the gauges
        ids = [gauge['loc_id'] for gauge in gauges]
        
        # get all the current levels from web CSVs 
        # debug:
        # latest_levels = {'116008':'0.01', '133112': \
        # '0.239', '234268': '5.224', '372871': '0.075'}
        latest_levels = retrieve_latest_from_csv.scrape_current_levels(ids) 
        
        excluded_gauges = ['116008'] # some seem to be wrongly calibrated!

        for gauge in gauges.rewind():
                
            if (gauge['loc_id'] in latest_levels) and \
            (gauge['loc_id'] not in excluded_gauges):
                current_scaled = float(latest_levels[gauge['loc_id']]) / \
                float(gauge['avg_level'])
                current_scaled = float("{0:.4f}".format(current_scaled))
                dbc.update({'loc_id': gauge['loc_id']}, {'$set': \
                    {'current_scaled': current_scaled, \
                    'current':latest_levels[gauge['loc_id']] }})
                
        # now take the n highest levels and watch these locations with Twitter:
        observed = dbc.find({}, { 'current_scaled': 1, 'bounding_box': 1, \
            'loc_id': 1, '_id': 0 }).sort([('current_scaled', -1)]).limit(25)

        prefix = ''
        for document in observed:
        
            coords = [str(coord) for coord in document['bounding_box']]
            query += prefix + ','.join(coords)
            prefix = ','

    elif (filtering == 'locfrance'):

        # set up gauges locations        
        print('processing france ...')
        exit()
        # get all gauges averages:
        
        # retrieve the location IDs of all the gauges
        
        # get all the current levels from web CSVs 
        
                
        # now take the n highest levels and watch these locations with Twitter:
     

        prefix = ''
        for document in observed:
        
            coords = [str(coord) for coord in document['bounding_box']]
            query += prefix + ','.join(coords)
            prefix = ',' 
    
    elif (filtering == 'locations'):


        dbc = get_dbc('Twitter', 'gauges')
        
        print('processing gauge levels...')
        # get all gauges averages:
        gauges = dbc.find({}, {'loc_id':1,'avg_level':1, '_id':0})
        
        # retrieve the location IDs of all the gauges
        ids = [gauge['loc_id'] for gauge in gauges]
        
        # get all the current levels from web CSVs 
        # debug:
        # latest_levels = {'116008':'0.01', '133112': \
        # '0.239', '234268': '5.224', '372871': '0.075'}
        latest_levels = retrieve_latest_from_csv.scrape_current_levels(ids) 
        
        excluded_gauges = ['116008'] # some seem to be wrongly calibrated!

        for gauge in gauges.rewind():
                
            if (gauge['loc_id'] in latest_levels) and \
            (gauge['loc_id'] not in excluded_gauges):
                current_scaled = float(latest_levels[gauge['loc_id']]) / \
                float(gauge['avg_level'])
                current_scaled = float("{0:.4f}".format(current_scaled))
                dbc.update({'loc_id': gauge['loc_id']}, {'$set': \
                    {'current_scaled': current_scaled, \
                    'current':latest_levels[gauge['loc_id']] }})
                
        # now take the n highest levels and watch these locations with Twitter:
        observed = dbc.find({}, { 'current_scaled': 1, 'bounding_box': 1, \
            'loc_id': 1, '_id': 0 }).sort([('current_scaled', -1)]).limit(25)

        prefix = ''
        for document in observed:
        
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


    
