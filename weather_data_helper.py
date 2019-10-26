# -*- coding: utf-8 -*-
"""
Created on Mon Nov  5 12:52:58 2018

@author: micha
"""
import requests as rq
import pandas as pd
import json
import datetime


class WeatherApiHelper:
    
    def __init__(self):
        self.__api_key = "API_KEY"
        self.__api_url_basic = "http://api.openweathermap.org/data/2.5/{}&APPID=" + self.__api_key
    
    #api.openweathermap.org/data/2.5/weather?q={city name}
    #api.openweathermap.org/data/2.5/weather?q={city name},{country code}
    def get_by_city_name(self, city_name, city_code = None):
        path = ""
        if city_code != None:
            path = self.__api_url_basic.format("weather?q=" + str(city_name) + "," + str(city_code))
        else:
            path = self.__api_url_basic.format("weather?q=" + str(city_name))
            
        content = rq.get(path).content  
        return content
    
    #api.openweathermap.org/data/2.5/weather?id=2172797
    def get_by_city_id(self, city_id):
        path = self.__api_url_basic.format("weather?id=" + str(city_id))
        content = rq.get(path).content
        return content
    
    #api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon} 
    def get_by_coordinates(self, lat, lon):
        path = self.__api_url_basic.format("weather?lat=" + str(lat) + "&lon=" + str(lon))
        content = rq.get(path).content
        return content
    
    #http://api.openweathermap.org/data/2.5/box/city?bbox=12,32,15,37,10
    #[lon-left,lat-bottom,lon-right,lat-top,zoom]
    def get_by_rectangle_zone(self, box):
        ids = ",".join(map(str, box))
        path = self.__api_url_basic.format("box/city?bbox=" + ids)
        content = rq.get(path).content
        return content
    
    #http://api.openweathermap.org/data/2.5/find?lat=55.5&lon=37.5&cnt=10
    def get_by_cicle(self, lat, lon, cnt):
        path = self.__api_url_basic.format("find?lat=" + str(lat) + "&lon=" + str(lon) + "&cnt=" + str(cnt))
        content = rq.get(path).content
        return content
    
    #http://api.openweathermap.org/data/2.5/group?id=524901,703448,2643743&units=metric 
    def get_by_several_city_ids(self, ids_list):
        if(len(ids_list) > 20):
            raise Exception("Max 20 ids")
        
        ids = ",".join(map(str, ids_list))
        path = self.__api_url_basic.format("group?id=" + ids + "&units=metric")
        content = rq.get(path).content
        return content


"""
    coord
        coord.lon City geo location, longitude
        coord.lat City geo location, latitude
    weather (more info Weather condition codes)
        weather.id Weather condition id
        weather.main Group of weather parameters (Rain, Snow, Extreme etc.)
        weather.description Weather condition within the group
        weather.icon Weather icon id
    base Internal parameter
    main
        main.temp Temperature. Unit Default: Kelvin, Metric: Celsius, Imperial: Fahrenheit.
        main.pressure Atmospheric pressure (on the sea level, if there is no sea_level or grnd_level data), hPa
        main.humidity Humidity, %
        main.temp_min Minimum temperature at the moment. This is deviation from current temp that is possible for large cities and megalopolises geographically expanded (use these parameter optionally). Unit Default: Kelvin, Metric: Celsius, Imperial: Fahrenheit.
        main.temp_max Maximum temperature at the moment. This is deviation from current temp that is possible for large cities and megalopolises geographically expanded (use these parameter optionally). Unit Default: Kelvin, Metric: Celsius, Imperial: Fahrenheit.
        main.sea_level Atmospheric pressure on the sea level, hPa
        main.grnd_level Atmospheric pressure on the ground level, hPa
    wind
        wind.speed Wind speed. Unit Default: meter/sec, Metric: meter/sec, Imperial: miles/hour.
        wind.deg Wind direction, degrees (meteorological)
    clouds
        clouds.all Cloudiness, %
    rain
        rain.3h Rain volume for the last 3 hours
    snow
        snow.3h Snow volume for the last 3 hours
    dt Time of data calculation, unix, UTC
    sys
        sys.type Internal parameter
        sys.id Internal parameter
        sys.message Internal parameter
        sys.country Country code (GB, JP etc.)
        sys.sunrise Sunrise time, unix, UTC
        sys.sunset Sunset time, unix, UTC
    id City ID
    name City name
    cod Internal parameter
"""

class WeatherDataParser:
        
    def _get_weather_row(self, weather_data, callsign):        
        weather_dict = {
                "date_time": pd.to_datetime(weather_data.get("dt"), unit ="s"),
                "lon": weather_data.get('coord', {}).get('Lon') if self.__is_list else weather_data.get('coord', {}).get('lon'),
                "lat": weather_data.get('coord', {}).get('Lat') if self.__is_list else weather_data.get('coord', {}).get('lat'),
                "weather_id": weather_data.get('weather', [{}])[0].get('id'),
                "weather_main": weather_data.get('weather', [{}])[0].get('main'),
                "weather_desc": weather_data.get('weather', [{}])[0].get('description'),
                "temp": weather_data.get('main', {}).get('temp'),
                "temp_min": weather_data.get('main', {}).get('temp_min'),
                "temp_max": weather_data.get('main', {}).get('temp_max'),
                "humidity": weather_data.get('main', {}).get('humidity'),
                "pressure": weather_data.get('main', {}).get('pressure'),
                "visibility":  weather_data.get('visibility'),
                "rain": weather_data.get('rain') if self.__is_list else weather_data.get('rain', {}).get('3h'),
                "snow": weather_data.get('snow') if self.__is_list else weather_data.get('snow', {}).get('3h'),
                "clouds": weather_data.get('clouds', {}).get('today') if self.__is_list else weather_data.get('clouds', {}).get('all'),
                "wind_speed": weather_data.get('wind', {}).get('speed'),
                "wind_deg": weather_data.get('wind', {}).get('deg'),
                "create_date": datetime.datetime.now()
                }
        row = pd.DataFrame(weather_dict, index=[0])
        row["callsign"] = callsign
        return row
    
    def get_weather_data_frame(self, json_content, callsign):
        weather_data = json.loads(json_content)
        
        self.__is_list = "list" in weather_data.keys()
        
        if(self.__is_list):
            frames_list = []
            weather_data = weather_data.get("list")
            for i in range(0, len(weather_data)):
                row = self._get_weather_row(weather_data[i], callsign)
                frames_list.append(row)
            
            concated = pd.concat(frames_list)
            return concated
        else:
            row = self._get_weather_row(weather_data, callsign)
            return row
        


#Test     

#w_h = WeatherApiHelper()
#content = w_h.get_by_city_name("London")
#content = w_h.get_by_coordinates(30,40)
#content = w_h.get_by_rectangle_zone([20,30,40,50,4])
#weather_data = json.loads(content)
#print(json.dumps(weather_data, indent=2))
#w_p = WeatherDataParser()
#row = w_p.get_weather_data_frame(content)

                
        
    