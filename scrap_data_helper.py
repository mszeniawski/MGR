# -*- coding: utf-8 -*-
"""
Created on Tue Mar 19 20:47:45 2019

@author: micha
"""

from bs4 import BeautifulSoup
from selenium import webdriver
from time import sleep
import random
import pandas as pd
from selenium.common.exceptions import TimeoutException


class ScrapDataHelper:

    def create_urls_dict(self, sky_frame, how_many=None):
        callsign_dict = sky_frame.loc[sky_frame['callsign'] != "", ['callsign']]
        callsign_dict_not_missing = pd.Series(callsign_dict["callsign"].unique())
        if how_many is not None:
            callsign_dict_not_missing = callsign_dict_not_missing.head(how_many)
        urls_dict = callsign_dict_not_missing.apply(lambda x: ("https://flightaware.com/live/flight/" + x).strip())
        dictionary = dict(zip(callsign_dict_not_missing, urls_dict))
        return dictionary
    
    def get_dates_info(self, html_content):
        soup = BeautifulSoup(html_content, 'html.parser')
        dates_table = soup.find_all("div", {"class": "flightPageSummaryTimes"})  
        departure_date = dates_table[1].find("span", {"class": "flightPageSummaryDepartureDayAbbrv"}).text
        departure_info = dates_table[1].find("span", {"class": "flightPageDepartureDelayStatus"}).text
        arrival_date = dates_table[1].find("span", {"class": "flightPageSummaryArrivalDayAbbrv"}).text
        arrival_info = dates_table[1].find("span", {"class": "flightPageArrivalDelayStatus"}).text
        
        return [departure_date, departure_info, arrival_date, arrival_info]
        
    def get_airport_info(self, html_content):
        soup = BeautifulSoup(html_content, 'html.parser')
        airport_table = soup.find_all("div", {"class": "flightPageSummaryAirports"})
        origin_code = airport_table[1].find("div", {"class": "flightPageSummaryOrigin"}) \
            .find("span", {"class": "flightPageSummaryAirportCode"}).text.replace("\n", "").strip()
        origin_city = airport_table[1].find("div", {"class": "flightPageSummaryOrigin"}) \
            .find("span", {"class": "flightPageSummaryCity"}).text.replace("\n", "").strip()
        destination_code = airport_table[1].find("div", {"class": "flightPageSummaryDestination"}) \
            .find("span", {"class": "flightPageSummaryAirportCode"}).text.replace("\n", "").strip()
        destination_city = airport_table[1].find("div", {"class": "flightPageSummaryDestination"}) \
            .find("span", {"class": "flightPageSummaryCity"}).text.replace("\n", "").strip()
        
        return [origin_code, origin_city, destination_code, destination_city]
        
    def get_flight_times_page_content(self, url):
        chromeOptions = webdriver.ChromeOptions()
        prefs = {'profile.managed_default_content_settings.images':2}
        chromeOptions.add_argument('--headless')
        chromeOptions.add_argument('--no-sandbox')
        chromeOptions.add_argument('--disable-dev-shm-usage')
        chromeOptions.add_argument('--dns-prefetch-disable')
        chromeOptions.add_experimental_option("prefs", prefs)
        html_source = None
        
        while True:
            try:
                driver = webdriver.Chrome("/usr/bin/chromedriver", chrome_options=chromeOptions)
                driver.set_page_load_timeout(30)
                driver.get(url)
                html_source = driver.execute_script("return document.getElementsByTagName('html')[0].innerHTML")
                driver.quit()
                break
            except TimeoutException as ex:
                html_source = None
                print("Timeout exception. Wait and reload driver...")
                driver.quit()
        return html_source
    
    def has_time(self, input_string):
        return any(char.isdigit() for char in input_string)
    
    def validate_data(self, flight_table_len, actual_times, scheduled_times):
        actual_times_valid = list(map(self.has_time, actual_times)) == 4*[True]
        scheduled_times_valid = list(map(self.has_time, scheduled_times)) == 4*[True]
        if flight_table_len == 4 and len(actual_times) == 4 and actual_times_valid and len(scheduled_times) == 4 and scheduled_times_valid:
            return True
        else: return False
    
    def get_flight_times_list(self, html_content, skipNotLiveInfo = True):
        soup = BeautifulSoup(html_content, 'html.parser')
        flight_table = soup.find_all("div", {"class": "flightPageDataTableContainer"})
        actual_times = []
        scheduled_times = []
        flight_table_len = len(flight_table)
        if flight_table_len > 2:
            search_table = None
            if skipNotLiveInfo and flight_table_len == 3:
                return None
            
            if flight_table_len == 3:
                search_table = flight_table[0]
                
            if flight_table_len == 4:
                search_table = flight_table[1]
                
            if search_table is not None:
                #Actual times
                actual_times_divs = search_table.find_all("div", {"class": "flightPageDataActualTimeText"})
                for i in range(0, len(actual_times_divs)):
                    time = actual_times_divs[i]
                    time_parsed = time.text.replace("\n", "").strip()
                    actual_times.append(time_parsed)
    
                scheduled_times_div = search_table.find_all("div", {"class": "flightPageDataAncillaryText"})
                for i in range(0, len(scheduled_times_div)):
                    time = scheduled_times_div[i]
                    time_parsed = time.text.replace("\n", "").strip()
                    if "Scheduled" in time_parsed:
                        time_parsed = time_parsed.replace("Scheduled", "").strip()
                        scheduled_times.append(time_parsed)
                
                isValid = self.validate_data(flight_table_len, actual_times, scheduled_times)
                
                if isValid == False:
                    return None
        else:
            return None
        
        return [actual_times, scheduled_times]
    
    def create_df_row(self, times_list, airports_info, dates_info, flight_name):
        actual_times = times_list[0]
        scheduled_times = times_list[1]
        
        actual_times += ['--'] * (4 - len(actual_times))
        scheduled_times += ['--'] * (4 - len(scheduled_times))
        
        keys_actual = ["gate_departure_actual", "takeoff_actual", "landing_actual", "gate_arrival_actual"]
        keys_scheduled = ["gate_departure_scheduled", "takeoff_scheduled", "landing_scheduled", "gate_arrival_scheduled"]
        keys_airports_info = ["origin_code", "origin_city", "destination_code", "destination_city"]
        keys_dates_info = ["departure_date", "departure_info", "arrival_date", "arrival_info"]
        
        dictionary_actual = dict(zip(keys_actual, actual_times))
        dictionary_scheduled = dict(zip(keys_scheduled, scheduled_times))
        dictionary_airports = dict(zip(keys_airports_info, airports_info))
        dictionary_dates = dict(zip(keys_dates_info, dates_info))
        
        actual_times_row = pd.DataFrame(dictionary_actual, index = [0])
        scheduled_times_row = pd.DataFrame(dictionary_scheduled, index = [0])
        airports_row = pd.DataFrame(dictionary_airports, index = [0])
        dates_row = pd.DataFrame(dictionary_dates, index = [0])
        
        result = pd.concat([airports_row, dates_row, actual_times_row, scheduled_times_row], axis=1)
        result["callsign"] = flight_name
        return result
    
    
    def get_flight_time_details(self, url, flight_name, skipNotLiveInfo = True):
        html = self.get_flight_times_page_content(url)
        if html is None:
            return None
        times_list = self.get_flight_times_list(html, skipNotLiveInfo)
        if times_list is None:
            return None
        
        airports_info = self.get_airport_info(html)
        dates_info = self.get_dates_info(html)
        df_row = self.create_df_row(times_list, airports_info, dates_info, flight_name)
        print("Page scrapped for: " + url)
        return df_row
