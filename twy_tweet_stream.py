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

# get the Twitter API app Oauth tokens
import sys
TWITDIR = 'U:\Documents\Project\demoapptwitter'
sys.path.insert(0, TWITDIR)
import config

# get fn updating water levels:
SCRAPEDIR = 'U:\Documents\Project\scrape'
sys.path.insert(0, SCRAPEDIR)
import parse_xml_locs, retrieve_latest_from_csv


# implement our extension to Twythons streamer
class TwitterStream(TwythonStreamer):
    '''Set up a queue for the tweets. Provides information on what to do
     when a tweet is received and on errors.'''
    def __init__(self, consumer_key, consumer_secret, token, token_secret, \
        tqueue):
        self.tweet_queue = tqueue
       
        # pass credentials to the parent class for authenticating:
        super(TwitterStream, self).__init__(consumer_key, consumer_secret, \
            token, token_secret)

    def on_success(self, data):
        if 'text' in data:
            self.tweet_queue.put(data)

    def on_error(self, status_code, data):
        logging.warning('code: ' + str(status_code))
        
        # Uncomment to stop trying to get data because of the error
        self.disconnect()
        logging.info('exited after disconnect.' )


def stream_tweets(tweets_queue, querystring, filter='follow'):
    ''' samples the stream API and handles errors'''
    logging.info(querystring)

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

    # choose which filter and query will be streamed
    filters = ['follow', 'locations', 'locations_disable_sepa', 'track']
    query = ''

    # IMPORTANT: set the Twitter Stream type of query here.
    # This sets out what type of search you will request from the Twitter API
    filtering = filters[2]

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
    
    elif (filtering[0:9] == 'locations'):

        # import this Env Agency wrapper to get bounding boxes for risk areas
        import api_wrapper

        # allow disabling of scottish areas:
        if filtering[-12:] != 'disable_sepa':

            dbc = get_dbc('Twitter', 'gauges')
            print('processing gauge levels...')
         
            # get all gauges averages:
            gauges = dbc.find({}, {'loc_id':1,'avg_level':1, '_id':0})
            
            # retrieve the location IDs of all the gauges
            ids = [gauge['loc_id'] for gauge in gauges]
            
            # get all the current levels from web CSVs on SEPA website
            # debug: latest_levels = {'116008':'0.01', '133112': '0.239'}
            latest_levels = retrieve_latest_from_csv.scrape_current_levels(ids) 
            logging.info('retrieved latest from CSV: ' + str(len(latest_levels)))
            for gauge in gauges.rewind():
                    
                if (gauge['loc_id'] in latest_levels) and \
                (gauge['loc_id'] not in config.excluded_gauges):
                    current_scaled = float(latest_levels[gauge['loc_id']]) / \
                    float(gauge['avg_level'])
                    current_scaled = float("{0:.4f}".format(current_scaled))
                    dbc.update({'loc_id': gauge['loc_id']}, {'$set': \
                        {'current_scaled': current_scaled, \
                        'current':latest_levels[gauge['loc_id']] }})
                    
            # now take the n highest levels and watch these locations with Twitter:
            observed = dbc.find({}, { 'current_scaled': 1, 'bounding_box': 1, \
                'loc_id': 1, '_id': 0 }).sort([('current_scaled', -1)]).limit(14)


            logging.info('observing DB query completed. ')

            prefix = '' # for separating querystring items with a comma
            for document in observed:
                
                coords = [str(coord) for coord in document['bounding_box']]
                query += prefix + ','.join(coords)
                prefix = ',' # after first item we need a comma every time!


        '''http://www.mapdevelopers.com/geocode_bounding_box.php
        manually add priority places 
         notts river greet;loughborough - flood alert 14/06/ 
          1.274237, 52.734273, -1.168320, 52.794977
        stafford, cheshire, 16/06/ added. Flood alerts.
        EXTRAS = '-1.789281,52.692522,-1.454885,52.860338,-2.169683,52.687947,
        -1.972616,52.807448,-2.953292,53.255697,-2.914123535,53.28841066,'
        '''
        #
        # Import some risk areas from latest Env Agency severity warning list. 
        apiw = api_wrapper.ApiWrapper()
        risk_area_urls = apiw.get_risk_areas(min_severity=4, max_areas=12)
        b_boxes = apiw.get_boxes(risk_area_urls) # list with 4 coord-bound boxes in
        if not b_boxes:
            if not query:
                logging.info('No query could be built as EnvAgency bounding boxes empty. ') 
                exit()
        else:
            # convert these boxes to a string and append to any query we may have already
            b_boxes = ','.join(map(str, b_boxes))

            # build a valid twitter-api string of locations, defined by bounding boxes
            if not query:
                query = b_boxes
            else:
                query = query + ',' + b_boxes
        
        query = ''.join(query.split()) # remove any extra whitespace

        # in case something went wrong, just use cached query string
        cached_query = 'cached.txt'
        if len(query) > 100:
            logging.info('cache writing...')
            # all is OK so cache this query to a file
            with open(cached_query, "w") as text_file:
                print(query, file=text_file)
                logging.info('...cache written.')

        else:
            # load from file
            with open(cached_query, "r") as text_file:
                query = text_file.read()
                logging.info('using cached as something went wrong.')

    else:
        # TODO: keyword tracking if needed.
        pass 

    # Store the Twitter Stream of tweets in a remote db via pymongo
    # dbc = get_dbc('database', 'streamedtweets', config.MONGO_URI)
    # changed 20/6/16 to new database (old was localstreamedtweets)
    collection_name = 'ea_risk_tweets'
    dbc = get_dbc('Twitter', collection_name) # temp use localhost

    # set up the queue & htread to make it more robust:
    tweet_queue = Queue()
    logging.info('passing to twitter API; db.collection:' + collection_name)

    # setup a thread whose functionality is the target:
    t = Thread(name='Stream-Tweets', target=stream_tweets, \
        args=(tweet_queue, query, filtering), daemon=True)
    t.start() # NB since it is a daemon, we don't need to use t.join() .

    # infinite loop that stores the tweets from the queue:
    process_tweets(dbc, tweet_queue)


    
