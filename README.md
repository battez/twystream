# twystream

Takes locations as bounding boxes that are submitted to Twitter Streaming API to continuously monitor for intersecting social geodata. (i.e. tweets with their **geotag** bounding box overlapping one of the locations).

Uses Twython library for robust handling of the Streaming API querying.

Logs locations searched.

## how to run
Call **twy_tweet_stream.py** with python. 
- uses get_dbc()  to connect to a MongoDB database for storing tweets retrieved.
- To access Twitter, you need API access, adjust the directory paths for your system, including to a file config.py with your Twitter OAuth access keys, token and secrets:

```
# OAuth credentials 
ul_consumer_key = 'YOURKEYHERE'
ul_consumer_secret = 'YOURKEYHERE'
ul_access_token = 'YOURKEYHERE'
ul_access_secret = 'YOURKEYHERE'
```

