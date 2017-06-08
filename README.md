# twystream

Takes locations as bounding boxes that are submitted to Twitter Streaming API to continuously monitor for intersecting social geodata. (i.e. tweets with their **geotag** bounding box overlapping one of the locations).

Uses Twython library for robust handling of the Streaming API querying.

Logs locations searched.

## how to run

Call **twy_tweet_stream.py** with python. This requires a couple of simple scripts in order to update and collect the locations from agency data feeds, SEPA script is in repository: https://github.com/battez/scraping and Environment Agency (EA) is in **api_wrapper.py** 

- first twy_tweet_stream polls locations for river levels. This makes use of get_dbc() to get cached MongoDB collection of SEPA gauges - gauge IDs and average levels.
- then uses a function in script https://github.com/battez/scraping/blob/master/retrieve_latest_from_csv.py to get latest from SEPA.
- for England and Wales EA script **api_wrapper.py**, which collects latest flood risks from their API: 
```
# class ApiWrapper() function is called with a min. severity level and max. no. areas to retrieve:
get_risk_areas(min_severity=4, max_areas=10)
```
- To access Twitter, you need API access, adjust the directory paths for your system, including to a file config.py with your Twitter OAuth access keys, token and secrets:

```
# OAuth credentials 
ul_consumer_key = 'YOURKEYHERE'
ul_consumer_secret = 'YOURKEYHERE'
ul_access_token = 'YOURKEYHERE'
ul_access_secret = 'YOURKEYHERE'
```
- also uses get_dbc()  to connect to a MongoDB collection for storing tweets retrieved.

