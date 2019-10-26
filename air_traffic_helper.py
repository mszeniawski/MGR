# -*- coding: utf-8 -*-
"""
Created on Mon Nov  5 13:52:19 2018

@author: micha
"""

from opensky_api import OpenSkyApi
import pandas as pd
import requests as rq
import json
import time
from datetime import datetime
import re

class OpenSkyHelper:
    
    def __init__(self):
        self.__api_base_path = "https://opensky-network.org/api/{}"
        self.__user = "USER_ID"
        self.__pass = "USER_PASS"
    
    def get_last_states(self):
        s = None
        while True:
            try:
                api = OpenSkyApi()
                s = api.get_states()
                break
            except:
                s = None
                print("Get states error. Reload...")
                
        columns = s.states[0].keys
        sky_list = [state.__dict__ for state in s.states]
        sky_frame = pd.DataFrame(sky_list, columns = columns)
        del(sky_frame["sensors"])
        sky_frame["time_position"] = pd.to_datetime(sky_frame["time_position"], unit ="s")
        sky_frame["last_contact"] = pd.to_datetime(sky_frame["last_contact"], unit ="s")
        sky_frame["create_date"] = datetime.now()
        
        return sky_frame
    
    # opensky-network.org/api/flights/all?begin=1517227200&end=1517230800
    # max 2 hours interval
    def get_flights_by_time_interval(self, begin, end):
        begin_date = self._parse_string_to_date(begin)
        end_date = self._parse_string_to_date(end)
        diff = end_date - begin_date
        days, seconds = diff.days, diff.seconds
        hours = days * 24 + seconds // 3600
        if(hours < 0 or hours > 2):
            raise Exception("Max 2 hours interval")
        
        begin = self._get_epoch_timestamp(begin)
        end = self._get_epoch_timestamp(end) 
        
        query = {'begin': begin, 'end': end}
        api_path = "/flights/all"
        path = self.__api_base_path.format(api_path)
        response = rq.get(path, params=query, auth=(self.__user, self.__pass))
        return self._prepareDataFrame(response)
    
    # opensky-network.org/api/flights/aircraft?icao24=3c675a&begin=1517184000&end=1517270400
    # max 30 days interval
    def get_flights_by_aircraft(self, icao24, begin, end):
        begin_date = self._parse_string_to_date(begin)
        end_date = self._parse_string_to_date(end)
        diff = (end_date - begin_date).days
        if(diff < 0 or diff > 30):
            raise Exception("Max 30 days interval")
        
        begin = self._get_epoch_timestamp(begin)
        end = self._get_epoch_timestamp(end)        
        
        query = {'icao24': icao24,'begin': begin, 'end': end}
        api_path = "/flights/aircraft"
        
        path = self.__api_base_path.format(api_path)
        response = rq.get(path, params=query, auth=(self.__user, self.__pass))
        return self._prepareDataFrame(response)
    
    # opensky-network.org/api/flights/arrival?airport=EDDF&begin=1517227200&end=1517230800
    # max 7 days interval
    def get_arrivals_by_airport(self, airport, begin, end):
        begin_date = self._parse_string_to_date(begin)
        end_date = self._parse_string_to_date(end)
        diff = (end_date - begin_date).days
        if(diff < 0 or diff > 7):
            raise Exception("Max 7 days interval")
            
        begin = self._get_epoch_timestamp(begin)
        end = self._get_epoch_timestamp(end)
        
        query = {'airport': airport,'begin': begin, 'end': end}
        api_path = "/flights/arrival"
        
        path = self.__api_base_path.format(api_path)
        response = rq.get(path, params=query, auth=(self.__user, self.__pass))
        return self._prepareDataFrame(response)
    
    # opensky-network.org/api/flights/departure?airport=EDDF&begin=1517227200&end=1517230800
    # max 7 days interval
    def get_departures_by_airport(self, airport, begin, end):
        begin_date = self._parse_string_to_date(begin)
        end_date = self._parse_string_to_date(end)
        diff = (end_date - begin_date).days
        if(diff < 0 or diff > 7):
            raise Exception("Max 7 days interval")
            
        begin = self._get_epoch_timestamp(begin)
        end = self._get_epoch_timestamp(end)
        
        query = {'airport': airport,'begin': begin, 'end': end}
        api_path = "/flights/departure"
        
        path = self.__api_base_path.format(api_path)
        response = rq.get(path, params=query, auth=(self.__user, self.__pass))
        return self._prepareDataFrame(response)
    
    # "2000-01-01 12:34:00"
    def _get_epoch_timestamp(self, date_string):
        conv_time = int(time.mktime(time.strptime(date_string, '%Y-%m-%d %H:%M:%S')))
        return str(conv_time)
            
    def _parse_string_to_date(self, data_string):
        parsed_date = datetime.strptime(data_string, '%Y-%m-%d %H:%M:%S')
        return parsed_date
    
    def _convert_to_underscore(self, value):
        s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', value)
        return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()
    
    def _prepareDataFrame(self, response):
        content = json.loads(response.content)
        frame = pd.DataFrame(content)
        frame_cols = [self._convert_to_underscore(x) for x in list(frame)]
        frame.columns = frame_cols
        frame["first_seen"] = pd.to_datetime(frame["first_seen"], unit ="s")
        frame["last_seen"] = pd.to_datetime(frame["last_seen"], unit ="s")
        
        return frame

#Test

#helper = OpenSkyHelper()
#frame = helper.get_last_states()
#frame = helper.get_flights_by_time_interval("2018-11-04 12:00:00", "2018-11-04 13:00:00")
#frame = helper.get_flights_by_aircraft("3c675a", "2018-11-04 12:00:00", "2018-11-04 15:00:00")
#frame = helper.get_arrivals_by_airport("EDDF", "2018-11-04 12:00:00", "2018-11-04 15:00:00")
#frame = helper.get_arrivals_by_airport("EDDF" ,"2018-11-04 12:00:00", "2018-11-06 12:00:00")
#frame = helper.get_departures_by_airport("EDDF" ,"2018-11-04 12:00:00", "2018-11-06 12:00:00")
#frame = helper.get_flights_by_aircraft("2018-11-04 12:00:00", "2018-11-04 15:00:00")
#print(frame)