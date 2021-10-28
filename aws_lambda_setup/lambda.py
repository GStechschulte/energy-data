import requests
import json
import pandas as pd
import time
import os
from get_data import get_data

energy_key = os.environ.get('energy_key','it didnt work')
weather_key = os.environ.get('weather_key','it didnt work')

def lambda_handler(event=True, context=True):

    print('Data fetch starts now')

    load = get_data(energy_key, weather_key)
    energy_generation = load.energy_generation('20191018T07Z') # pd df
    energy_demand = load.energy_demand('20191018T07Z') # pd df
    
    print('Data fetched')
    
    print(energy_generation.head(), '\n')
    print(25*'-', '\n')
    print(energy_demand.head())
   
    hydro = load.hydro_weather() # JSON format
    wind = load.wind_weather() # JSON format
    solar = load.solar_weather() # JSON format
   
    print(hydro, '\n')
    print(25*'-', '\n')
    print(wind, '\n')
    print(25*'-', '\n')
    print(solar)

lambda_handler()