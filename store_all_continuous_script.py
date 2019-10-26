# -*- coding: utf-8 -*-
"""
Created on Sat Jun 29 13:58:15 2019

@author: micha
"""
#uruchomienie : python skrypt.py 2000
import sys
import store_data_helper
from time import sleep
import random

wait_time_list = [i for i in range(10,21)]
sample_size = int(sys.argv[1])

while True:
    store_data_helper_ubuntu.store_all(sample_size);
    wait_time = random.choice(wait_time_list)
    print("Wait " + str(wait_time) + " minutes between samples...")
    sleep(wait_time*60)