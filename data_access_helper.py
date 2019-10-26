# -*- coding: utf-8 -*-
"""
Created on Wed Jul 31 20:17:17 2019

@author: micha
"""
import pandas as pd
import requests as rq
import store_data_helper
from dateutil.parser import parse
from bs4 import BeautifulSoup, re
from datetime import datetime
from pytz import timezone
from tzlocal import get_localzone

def clean_delay_data(delay_df):
    del(delay_df["_id"])
    delay_df["arrival_date"] = delay_df["arrival_date"].apply(lambda x: parse(x).strftime("%Y-%m-%d"))
    delay_df["departure_date"] = delay_df["departure_date"].apply(lambda x: parse(x).strftime("%Y-%m-%d"))
    
    def create_delay_time_column(x):
        splited = x.replace("(", "").replace(")", "")
        isLate = x.find("late") != -1
        isEarly = x.find("early") != -1
        isOnTime = not isLate and not isEarly
        if isOnTime:
            return 0
        else:
            minutes = 0
            times_arr = [int(s) for s in splited.split() if s.isdigit()]
            if len(times_arr) == 2:
                minutes = times_arr[0]*60 + times_arr[1]
            else:
                minutes = times_arr[0]
        if isLate:
            return -minutes
        else:
            return minutes

    delay_df["arrival_info_minutes"] = delay_df["arrival_info"].apply(create_delay_time_column)
    delay_df["departure_info_minutes"] = delay_df["departure_info"].apply(create_delay_time_column)
    del(delay_df["arrival_info"])
    del(delay_df["departure_info"])

    delay_df["destination_region"] = delay_df["destination_city"].str.strip().str.split(",", expand=True)[1]
    delay_df["destination_city"] = delay_df["destination_city"].str.strip().str.split(",", expand=True)[0]

    delay_df["origin_region"] = delay_df["origin_city"].str.strip().str.split(",", expand=True)[1]
    delay_df["origin_city"] = delay_df["origin_city"].str.strip().str.split(",", expand=True)[0]

    def convert24(str1):
        if str1[-2:] == "AM" and str1[:2] == "12":
            return "00" + str1[2:-2]   
        elif str1[-2:] == "AM":
            return str1[:-2] 
        elif str1[-2:] == "PM" and str1[:2] == "12":
            return str1[:-2]
        else:
            return str(int(str1[:2]) + 12) + str1[2:8]
    
    def create_time_column(value):
        cleaned = value.replace("\t", "").strip()
        added_space = cleaned[:5] + " " + cleaned[5:]
        splited = added_space.split(" ")
        splited = splited[:3]
        splited[0] = splited[0] + ":00"
        if("+" in splited[2] or "-" in splited[2]):
            splited[2] = "Etc/GMT" + splited[2][0] + str(int(splited[2]))

        joined = " ".join(splited)
        result = convert24(joined[:11]) + joined[11:]

        return result.strip()

    delay_df["gate_arrival_actual"] = delay_df["gate_arrival_actual"].apply(create_time_column)
    delay_df["gate_arrival_scheduled"] = delay_df["gate_arrival_scheduled"].apply(create_time_column)
    delay_df["gate_departure_actual"] = delay_df["gate_departure_actual"].apply(create_time_column)
    delay_df["gate_departure_scheduled"] = delay_df["gate_departure_scheduled"].apply(create_time_column)
    delay_df["landing_actual"] = delay_df["landing_actual"].apply(create_time_column)
    delay_df["landing_scheduled"] = delay_df["landing_scheduled"].apply(create_time_column)
    delay_df["takeoff_actual"] = delay_df["takeoff_actual"].apply(create_time_column)
    delay_df["takeoff_scheduled"] = delay_df["takeoff_scheduled"].apply(create_time_column)

    delay_df["gate_arrival_actual_timezone_code"] = delay_df["gate_arrival_actual"].str.split("\\s+", expand=True)[1]
    delay_df["gate_arrival_actual"] = delay_df["gate_arrival_actual"].str.split("\\s+", expand=True)[0]

    delay_df["gate_arrival_scheduled_timezone_code"] = delay_df["gate_arrival_scheduled"].str.split("\\s+", expand=True)[1]
    delay_df["gate_arrival_scheduled"] = delay_df["gate_arrival_scheduled"].str.split("\\s+", expand=True)[0]

    delay_df["gate_departure_actual_timezone_code"] = delay_df["gate_departure_actual"].str.split("\\s+", expand=True)[1]
    delay_df["gate_departure_actual"] = delay_df["gate_departure_actual"].str.split("\\s+", expand=True)[0]

    delay_df["gate_departure_scheduled_timezone_code"] = delay_df["gate_departure_scheduled"].str.split("\\s+", expand=True)[1]
    delay_df["gate_departure_scheduled"] = delay_df["gate_departure_scheduled"].str.split("\\s+", expand=True)[0]

    delay_df["landing_actual_timezone_code"] = delay_df["landing_actual"].str.split("\\s+", expand=True)[1]
    delay_df["landing_actual"] = delay_df["landing_actual"].str.split("\\s+", expand=True)[0]

    delay_df["landing_scheduled_timezone_code"] = delay_df["landing_scheduled"].str.split("\\s+", expand=True)[1]
    delay_df["landing_scheduled"] = delay_df["landing_scheduled"].str.split("\\s+", expand=True)[0]

    delay_df["takeoff_actual_timezone_code"] = delay_df["takeoff_actual"].str.split("\\s+", expand=True)[1]
    delay_df["takeoff_actual"] = delay_df["takeoff_actual"].str.split("\\s+", expand=True)[0]

    delay_df["takeoff_scheduled_timezone_code"] = delay_df["takeoff_scheduled"].str.split("\\s+", expand=True)[1]
    delay_df["takeoff_scheduled"] = delay_df["takeoff_scheduled"].str.split("\\s+", expand=True)[0]

    delay_df["gate_arrival_actual"] = delay_df["arrival_date"] + " " + delay_df["gate_arrival_actual"]
    delay_df["gate_arrival_scheduled"] = delay_df["arrival_date"] + " " + delay_df["gate_arrival_scheduled"]
    delay_df["gate_departure_actual"] = delay_df["departure_date"] + " " + delay_df["gate_departure_actual"]
    delay_df["gate_departure_scheduled"] = delay_df["departure_date"] + " " + delay_df["gate_departure_scheduled"]
    delay_df["landing_actual"] = delay_df["arrival_date"] + " " + delay_df["landing_actual"]
    delay_df["landing_scheduled"] = delay_df["arrival_date"] + " " + delay_df["landing_scheduled"]
    delay_df["takeoff_actual"] = delay_df["departure_date"] + " " + delay_df["takeoff_actual"]
    delay_df["takeoff_scheduled"] = delay_df["departure_date"] + " " + delay_df["takeoff_scheduled"]

    columns = [i for i in delay_df.columns if "timezone" in i]
    
    def get_zones_dict(df):
        selected = df[columns]
        temp_df = df["gate_arrival_actual_timezone_code"]
        for i in range(1, len(columns)):
            temp_df = temp_df.append(selected[columns[i]], ignore_index=True)
        zones = [z.lower() for z in temp_df.unique() if "GMT" not in z]
        zones_dict = {}
        for zone in zones:
            url = 'https://www.timeanddate.com/time/zones/' + zone
            page = rq.get(url)
            content = page.content
            soup = BeautifulSoup(content, 'html.parser')
            scraped_zone = soup.find_all("ul", {"class" : "clear"})
            if len(scraped_zone) > 0:
                p = re.compile(r'UTC [+-][0-9]{1,2}\b')
                search = p.search(scraped_zone[0].text)
                group = search.group(0)
                result = re.sub('[\s]', '', group)
                zones_dict[zone] = result.replace("UTC", "Etc/GMT")
        return zones_dict
            
    zones_dict = get_zones_dict(delay_df)

    for column in columns:
        condition = ~delay_df[str(column)].str.contains("GMT", na=False)
        delay_df.loc[condition,[str(column)]] = delay_df[condition][str(column)].str.lower().map(zones_dict)
        delay_df = delay_df.dropna()


    def localize_times(time_str, timezone_desc):
        fmt = '%Y-%m-%d %H:%M:%S'
        datetime_object = datetime.strptime(time_str, fmt)

        #additional clean
        timezone_desc = timezone_desc.replace("--", "-")
        timezone_desc = timezone_desc.replace("30", "")
        timezone_desc = timezone_desc.replace("545", "6")
        timezone_desc = timezone_desc.replace("Etc/GMT+13", "Pacific/Enderbury")
        
        etc_timezone = timezone(timezone_desc)
        loc_dt = etc_timezone.localize(datetime_object)
        local_tz = get_localzone() 
        return loc_dt.astimezone(local_tz).strftime(fmt)
    
    delay_df_copy = delay_df.copy()
    time_columns = ['gate_arrival_actual',
     'gate_arrival_scheduled',
     'gate_departure_actual',
     'gate_departure_scheduled',
     'landing_actual',
     'landing_scheduled',
     'takeoff_actual',
     'takeoff_scheduled']

    for tc in time_columns:
        delay_df_copy[str(tc)] = delay_df_copy.apply(lambda x: localize_times(x[str(tc)], x[str(tc) + "_timezone_code"]), axis=1)
    
    columns_to_delete = [i for i in delay_df_copy.columns if "timezone" in i]
    columns_to_delete.append("arrival_date")
    columns_to_delete.append("departure_date")

    for col in columns_to_delete:
        del(delay_df_copy[col])

    columns_order = ['callsign','origin_city', 'origin_region',
           'origin_code', 'destination_city','destination_region', 'destination_code',
           'gate_arrival_actual', 'gate_arrival_scheduled',
           'gate_departure_actual', 'gate_departure_scheduled',
           'landing_actual', 'landing_scheduled','takeoff_actual', 'takeoff_scheduled',
           'arrival_info_minutes', 'departure_info_minutes', 'sample_id']

    delay_df_copy = delay_df_copy[columns_order]

    delay_df_copy["arrival_info_minutes"] = delay_df_copy["arrival_info_minutes"].astype(int)
    delay_df_copy["departure_info_minutes"] = delay_df_copy["departure_info_minutes"].astype(int)
    delay_df_copy['gate_arrival_actual'] =  pd.to_datetime(delay_df_copy['gate_arrival_actual'], format='%Y-%m-%d %H:%M:%S')
    delay_df_copy['gate_arrival_scheduled'] =  pd.to_datetime(delay_df_copy['gate_arrival_scheduled'], format='%Y-%m-%d %H:%M:%S')
    delay_df_copy['gate_departure_actual'] =  pd.to_datetime(delay_df_copy['gate_departure_actual'], format='%Y-%m-%d %H:%M:%S')
    delay_df_copy['gate_departure_scheduled'] =  pd.to_datetime(delay_df_copy['gate_departure_scheduled'], format='%Y-%m-%d %H:%M:%S')
    delay_df_copy['landing_actual'] =  pd.to_datetime(delay_df_copy['landing_actual'], format='%Y-%m-%d %H:%M:%S')
    delay_df_copy['landing_scheduled'] =  pd.to_datetime(delay_df_copy['landing_scheduled'], format='%Y-%m-%d %H:%M:%S')
    delay_df_copy['takeoff_actual'] =  pd.to_datetime(delay_df_copy['takeoff_actual'], format='%Y-%m-%d %H:%M:%S')
    delay_df_copy['takeoff_scheduled'] =  pd.to_datetime(delay_df_copy['takeoff_scheduled'], format='%Y-%m-%d %H:%M:%S')
    
    return delay_df_copy

def get_cleaned_delay_data():
    delay_records = store_data_helper.get_data("DelayData")
    delay_df = pd.DataFrame.from_records(delay_records)
    return clean_delay_data(delay_df)


def get_cleaned_air_traffic_data():
    air_traffic_records = store_data_helper.get_data("AirTrafficData")
    air_traffic_df = pd.DataFrame.from_records(air_traffic_records)
    del(air_traffic_df["_id"])
    return air_traffic_df
    
def get_cleaned_weather_data():
    weather_records = store_data_helper.get_data("WeatherData")
    weather_df = pd.DataFrame.from_records(weather_records)
    del(weather_df["_id"])
    del(weather_df["rain"])
    del(weather_df["snow"])
    del(weather_df["visibility"])
    del(weather_df["lon"])
    del(weather_df["lat"])
    return weather_df

def get_final_data():
    delay_data_cleaned = get_cleaned_delay_data()
    weather_df = get_cleaned_weather_data()
    air_traffic_df = get_cleaned_air_traffic_data()
    
    sample_ids = delay_data_cleaned["sample_id"].unique()
    weather_df = weather_df[weather_df['sample_id'].isin(sample_ids)]
    air_traffic_df = air_traffic_df[air_traffic_df['sample_id'].isin(sample_ids)]
    
    data_merged_df = pd.merge(air_traffic_df, delay_data_cleaned, on=['callsign', 'sample_id'])
    data_merged_df = pd.merge(data_merged_df, weather_df, on=['callsign', 'sample_id'])
    data_merged_df["delayed_on_arrival"] = data_merged_df["arrival_info_minutes"] < 0
    data_merged_df["delayed_on_departure"] = data_merged_df["departure_info_minutes"] < 0
    
    return data_merged_df