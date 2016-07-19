""" functions to interact with APIs -- the Met Office:
http://www.metoffice.gov.uk/public/weather/observation/?tab=extremes
 and Env Agency flood api:
http://environment.data.gov.uk/flood-monitoring/doc/reference

"""
import json
from pprint import pprint
# basic logging for this task.
import logging
FORMAT = "%(asctime)-15s %(message)s"
logging.basicConfig(filename="uk_apis_log.txt", level=logging.INFO, format=FORMAT)

class ApiWrapper(object):

    def __init__(self):
        self.RISK_STEM_URL_EA = 'http://environment.data.gov.uk/flood-monitoring/'
        self.RISK_SUFFIX = 'id/floods?min-severity='
        super(ApiWrapper, self).__init__()
        

    def get_risk_areas(self, min_severity=4, max_areas=10):
        
        risk_areas = [] # list URLs
        url = self.RISK_STEM_URL_EA + self.RISK_SUFFIX + \
         str(min_severity)
        areas_json = self.get_json(url=url)

        if 'items' in areas_json:  
            # extract the worst severity areas:
            severity = 1 # most risky!
            while severity <= min_severity:

                # loop through severity_level in asc. order
                for row in areas_json['items']:
                    
                    if (int(row['severityLevel']) == severity) and ('floodArea' in row):

                        # only stop when reach required limit 
                        if len(risk_areas) < max_areas:
                            if 'polygon' in row['floodArea']:
                                # grab this URL as an area of risk:
                                risk_areas.append(\
                                    row['floodArea']['polygon'])
                        else:  
                            # stop...
                            break
                # try a less risky severity:
                severity += 1

        return risk_areas


    def process_areas():
        '''TODO: take each area and return its bounding box.
        I.e. return in format for twitter API location tracking. 
        '''
        locations = [] # list of tuples 
        return locations


    def make_bounding_box(self, geojson, margin=0):
        '''TODO: convert a multipolygon to its containing box.
        Pass optional margin to increase area of box.
        '''

        box = []
        return box


    def get_json(self, url=False, file='sample.json'):
        '''load some json from an API URL or a local file 
        (i.e. to test)'''  
        import urllib.request
        got_json = None

        if(url):
            try:
                response = urllib.request.urlopen(url)
                content = response.read()
            except:
                logging.error('url:'+url+ ' with get_json:')
            else:
                got_json = json.loads(content.decode('utf8'))

        else: # sample local file:        
            with open(file, 'r') as f:
                got_json = json.load(f)
     
        return got_json


    # explode() and get_box() adapted from
    # http://gis.stackexchange.com/questions/90553/
    def explode(self, coords):
        """Explode a GeoJSON geometry's coordinates object and yield coordinate tuples.
        As long as the input is conforming, the type of the geometry doesn't matter."""
        for e in coords:
            if isinstance(e, (float, int)):
                yield coords
                break
            else:
                for f in self.explode(e):
                    yield f


    def get_box(self, f):
        '''pass a GeoJSON feature to return a tuple bounding box'''
        # TODO: add a margin to increase the box if required
        # cf http://gis.stackexchange.com/questions/19760/
        #how-do-i-calculate-the-bounding-box-for-given-a-distance-and-latitude-longitude
        x, y = zip(*list(self.explode(f['geometry']['coordinates'])))
        return min(x), min(y), max(x), max(y)


    def get_boxes(self, risk_area_urls, untuple=True):
        if not risk_area_urls:
            logging.info('risk areas URLs was empty!')
            return []

        b_boxes = []

        # iterate thru these riskarea URLs and retrieve each bounding box 
        for url in risk_area_urls:
            
            geo_json = self.get_json(url=url)

            # FIXME: should possibly test for attribute type
            # FeatureCollection here
            try:
                if (len(geo_json['features']) == 1) and \
                (geo_json['features'][0] is not None):
                    # NB result will be form: bottomleft-topright 

                    # grow bounding box
                    # http://www.crowbarsolutions.com/adjusting-zoom-level-to-add-padding-to-map-bounding-boxes-bing-maps-v7/
                    b_box = self.get_box(geo_json['features'][0])
                    if len(b_box) == 4:
                        
                        b_boxes.append(b_box)
                  
                else:
                    raise ValueError('error with json structure') from error
            except TypeError:
                print('typeError is the JSON loading?')
        
        if untuple:
            # unpack the tuples into a list and TODO: format for length trim:
            b_boxes = [i for sub in b_boxes for i in sub]
            
        return b_boxes

# debugging: direct execution of this file:
if __name__ == '__main__':
    
    apiw = ApiWrapper()
    risk_area_urls = apiw.get_risk_areas()

    b_boxes = apiw.get_boxes(risk_area_urls) 
    print(b_boxes)
    pass
