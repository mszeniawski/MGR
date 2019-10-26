# -*- coding: utf-8 -*-
"""
Created on Wed May  1 21:28:12 2019

@author: micha
"""

from air_traffic_helper import OpenSkyHelper
from weather_data_helper import WeatherApiHelper, WeatherDataParser
from scrap_data_helper_ubuntu import ScrapDataHelper
import pandas as pd
from pymongo import MongoClient
import uuid

mongo_connection = 'MONGO_CONNECTION_STRING'

def get_air_traffic_frame():
    air_helper = OpenSkyHelper()
    sky_frame = air_helper.get_last_states()
    sky_frame = sky_frame[sky_frame["latitude"].notnull() & sky_frame["longitude"].notnull()]
    sky_frame = sky_frame.drop_duplicates(subset=['callsign'])
    return sky_frame

def store_air_traffic_data(sky_frame, sample_id):
    client = MongoClient(mongo_connection)
    db = client.mo1271_mgr_data
    air_traffic_collection = db.AirTrafficData
    sky_frame['time_position'].fillna('', inplace=True)
    sky_frame['sample_id'] = sample_id
    strip_string_columns(sky_frame)
    sky_frame_dict = sky_frame.to_dict('records')
    for rec in sky_frame_dict:
        air_traffic_collection.insert_one(rec)
    print("Air Traffic Stored")
    
def store_weather_data(sky_frame, sample_id):
    weather_helper = WeatherApiHelper()
    weather_parser = WeatherDataParser()
    w_frames = []
    def getWeather(df):
        lat = df["latitude"]
        lon = df["longitude"]
        callsign = df["callsign"]
        condtions = weather_helper.get_by_coordinates(lat, lon)
        w_d = weather_parser.get_weather_data_frame(condtions, callsign)
        w_frames.append(w_d)
        
    sky_frame.apply(getWeather, axis=1)
    condtions_df = pd.concat(w_frames, ignore_index=True)
    condtions_df['sample_id'] = sample_id
    strip_string_columns(condtions_df)
    client = MongoClient(mongo_connection)
    db = client.mo1271_mgr_data
    weather_collection = db.WeatherData
    condtions_df_dict = condtions_df.to_dict('records')
    for rec in condtions_df_dict:
        weather_collection.insert_one(rec)   
    print("Weather Stored")
    
def store_delay_data(sky_frame, sample_id):
    scrap_helper = ScrapDataHelper()
    rows = []
    sample_urls = scrap_helper.create_urls_dict(sky_frame)
    for numb, url in sample_urls.items():
        row = scrap_helper.get_flight_time_details(url, numb)
        if(row is not None):
            #print(url + " added to list")
            rows.append(row)
    delay_df = pd.concat(rows)
    delay_df['sample_id'] = sample_id
    strip_string_columns(delay_df)
    client = MongoClient(mongo_connection)
    db = client.mo1271_mgr_data
    delay_collection = db.DelayData
    delay_df_dict = delay_df.to_dict('records')
    for rec in delay_df_dict:
        delay_collection.insert_one(rec)   
    print("Delay Stored")
    
def strip_string_columns(data_frame):
    df_obj = data_frame.select_dtypes(['object'])
    data_frame[df_obj.columns] = df_obj.apply(lambda x: x.str.strip())

def remove_not_scraped_records(sample_id):
    client = MongoClient(mongo_connection)
    db = client.mo1271_mgr_data
    air_traffic_collection = db.AirTrafficData
    weather_collection = db.WeatherData
    delay_collection = db.DelayData
    
    data = list(delay_collection.find({"sample_id": sample_id}, {"callsign":1, "_id": 0}))
    callsign_list = [document["callsign"] for document in data]
    
    remove_query = { "$and":[{"callsign": { "$not": {"$in": callsign_list}}}, {"sample_id": sample_id}]}
    
    removed_air_traffic_data = air_traffic_collection.delete_many(remove_query)
    removed_weather_data = weather_collection.delete_many(remove_query)
    print(removed_air_traffic_data.deleted_count, " documents deleted from AirTrafficData.")
    print(removed_weather_data.deleted_count, " documents deleted from WeatherData.")
    
def store_all(sample_size = None):
    sample_id = str(uuid.uuid4())
    print("Sample ID = " + sample_id)
    sky_frame = get_air_traffic_frame()
    if sample_size is not None:
        sky_frame = sky_frame.sample(n=sample_size)
    store_air_traffic_data(sky_frame, sample_id)
    store_weather_data(sky_frame, sample_id)
    store_delay_data(sky_frame, sample_id)
    remove_not_scraped_records(sample_id)