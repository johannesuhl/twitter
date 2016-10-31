#This script streams tweets for a given geographic region ahd stores them as point features into a ESRI File Geodatabase.
#Johannes H. Uhl
#University of Colorado
#Department of Geography
#johannes.uhl@colorado.edu
#September 2015

from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener
import json
from geopy.geocoders import Nominatim
from geopy.distance import vincenty
import csv
import os, sys
import arcpy
from arcpy import env
import dateutil.parser
from dateutil.relativedelta import relativedelta

#-----------PARAMETERS-----------------------------------------------

#consumer key, consumer secret, access token, access secret from Twitter API
ckey = 'ADD_YOUR_TWITTER_API_CREDENTIALS_HERE'
csecret = 'ADD_YOUR_TWITTER_API_CREDENTIALS_HERE'
atoken = 'ADD_YOUR_TWITTER_API_CREDENTIALS_HERE'
asecret = 'ADD_YOUR_TWITTER_API_CREDENTIALS_HERE'

LOCATION_NAME = "Mexico City"
TIME_DIFF_TO_GMT = 5

GDB_PATH = r'G:\TWITTER'
GDB_NAME = 'MXDF.gdb'
FCL_NAME = 'MXDF1'

prj_file = r'PRJ_templateWGS84'

createNewGDB = True
createNewFCL = True

#-------------------------------------------------------------------

def is_string(obj):
    try:
        obj + ''
        return True
    except TypeError:
        return False

def byteify(input):
    if isinstance(input, dict):
        return {byteify(key):byteify(value) for key,value in input.iteritems()}
    elif isinstance(input, list):
        return [byteify(element) for element in input]
    elif isinstance(input, unicode):
        return input.encode('utf-8')
    else:
        return input

class listener(StreamListener):

    def on_data(self, data):
        #print data
        try:
            obj = json.loads(data)
            try:
                x = obj['coordinates']['coordinates'][0]
                y = obj['coordinates']['coordinates'][1]
            except:
                x=0.0
                y=0.0

            text = obj['text'].replace('\n',' ').replace('\r',' ')
            text_ascii = text.encode('ascii','replace')

            date1 = obj['created_at'] #is GMT!!!
            date2 = date1.split(' ')
            date3 = date2[0]+' '+date2[1]+' '+date2[2]+' '+date2[3]+' '+date2[5]
            date4 = dateutil.parser.parse(date3)
            date5 = date4 - relativedelta(hours=TIME_DIFF_TO_GMT) #local time!!
            

            #values_list = obj.values()
            #print values_list

            #tweets_list.append(obj)
            print str(x), str(y), text, date1

            # Create insert cursor for table
            fcl = GDB_PATH + os.sep + GDB_NAME + os.sep + FCL_NAME
            # Open an InsertCursor
            cursor = arcpy.da.InsertCursor(fcl,['x', 'y', 'tweet', 'date', 'SHAPE@XY'])
            row = (x, y, text_ascii, date5, (x, y))
            cursor.insertRow(row)
            del row
            del cursor
        except:
            print "error writing row!"
            True
       

#---------------------------------------------------------------------------------------------

geolocator = Nominatim()
location = geolocator.geocode(LOCATION_NAME)
#geometry = geolocator.geocode(LOCATION_NAME,'geometry')
print "address:", location.address
print "center:", (location.latitude, location.longitude)
#print geometry
raw = byteify(location.raw)
bb = raw['boundingbox']
twitter_loc = (float(bb[2]),float(bb[0]),float(bb[3]),float(bb[1]))
print "bounding box:", twitter_loc

ext_ew = vincenty((twitter_loc[0], twitter_loc[1]), (twitter_loc[2], twitter_loc[1]))
ext_sn = vincenty((twitter_loc[0], twitter_loc[1]), (twitter_loc[0], twitter_loc[3]))

print "extent:", ext_ew, 'E->W', 'x', ext_sn, 'N->S'          

fileSetupOK = True
if createNewGDB:

    try:
        arcpy.CreateFileGDB_management(GDB_PATH, GDB_NAME)
    except:
        print "GDB already exists."
        fileSetupOK = False

if createNewFCL:

    # Set workspace
    env.workspace = GDB_PATH + os.sep + GDB_NAME

    # delete FCL if exists:
    if arcpy.Exists(FCL_NAME):
        arcpy.Delete_management(FCL_NAME)
        
    # Set local variables
    out_path = env.workspace
    out_name = FCL_NAME
    geometry_type = "POINT"
    template = ""
    has_m = "DISABLED"
    has_z = "DISABLED"
    spatial_reference = prj_file
    # Execute CreateFeatureclass
    arcpy.CreateFeatureclass_management(out_path, out_name, geometry_type, template, has_m, has_z, spatial_reference)
    arcpy.AddField_management(env.workspace+os.sep+FCL_NAME, "x", "DOUBLE")
    arcpy.AddField_management(env.workspace+os.sep+FCL_NAME, "y", "DOUBLE")
    arcpy.AddField_management(env.workspace+os.sep+FCL_NAME, "tweet", "TEXT",'','',200)
    arcpy.AddField_management(env.workspace+os.sep+FCL_NAME, "date", "DATE",'','',35)


#-------------get tweets--------------------------------------------

def getTweets(ckey,csecret,atoken,asecret,twitter_loc):
    auth = OAuthHandler(ckey, csecret)
    auth.set_access_token(atoken, asecret)
    twitterStream = Stream(auth, listener())
    twitterStream.filter(locations=twitter_loc)

for i in range (0,100000): #we allow 100000 errors due to connection and API problems.
    try:    
        getTweets(ckey,csecret,atoken,asecret,twitter_loc)
    except:
        i+=1
        print "error", str(i)


