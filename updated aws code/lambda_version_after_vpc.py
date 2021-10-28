import requests
import json
import pandas as pd
import time
import os


energy_key = os.environ.get('energy_key','it didnt work')
weather_key = os.environ.get('weather_key','it didnt work')

def lambda_handler(event, context):

    try:
        print('First line of first try')
        hydro_url = 'https://api.eia.gov/series/?api_key={}&series_id=EBA.CAL-ALL.NG.WAT.H&start={}'.format(energy_key, '20191018T07Z')
        solar_url = 'https://api.eia.gov/series/?api_key={}&series_id=EBA.CAL-ALL.NG.SUN.H&start={}'.format(energy_key, '20191018T07Z')
        wind_url = 'https://api.eia.gov/series/?api_key={}&series_id=EBA.CAL-ALL.NG.WND.H&start={}'.format(energy_key, '20191018T07Z')

        print('URL ',hydro_url)
        print('URL ',solar_url)
        print('URL ',wind_url)
        
        hydro_response = requests.get(hydro_url).json()
        print('Hydro post done')
        solar_response = requests.get(solar_url).json()
        print('Solar post done')
        wind_response = requests.get(wind_url).json()
        print('Wind post done')
    except:
        print('Loading not successfull')
        
    try:
        hydro = dict(hydro_response['series'][0]['data'])
        solar = dict(solar_response['series'][0]['data'])
        wind = dict(wind_response['series'][0]['data'])

        
        energy_generation = pd.DataFrame.from_dict([hydro, solar, wind]).T
    
        energy_generation.rename(columns={0: 'hydro_MWh',
                                          1: 'solar_MWh',
                                          2: 'wind_MWh'}, inplace=True)
    
        energy_generation.index = pd.to_datetime(energy_generation.index, utc=True)
        print(energy_generation.head())
        print('energy_generation() done')
    except:
        print('Energy Generation: DataFrame generation didnt work')
    
    
    try:
        url = 'https://api.eia.gov/series/?api_key={}&series_id=EBA.CAL-ALL.D.H&start={}'.format(energy_key, '20191018T07Z')
        energy = requests.post(url).json()
        print('Energy post done')
        data = dict(energy['series'][0]['data'])
    
        demand = pd.DataFrame.from_dict(data, orient='index', columns=['demand_MWh'])
        demand.index = pd.to_datetime(demand.index, utc=True)
    
        print('energy_demand() done')

    except:
        print('Energy Demand: DataFrame generation didnt work')


    try:
        hydro_cities = {'Yosemite Valley': 7262586, 'Tahoe Vista': 5400943, 
                    'Lassen County': 5566544, 'Eldorado': 5947462}

        hydro_weather = {}

        for city, key in hydro_cities.items():
                url = 'https://api.openweathermap.org/data/2.5/weather?id={}&appid={}&units=metric'.format(key, weather_key)
                response = requests.get(url)
                print('hydro_cities get done')
                city_weather = json.loads(response.text)
                hydro_weather[city] = city_weather
                #time.sleep(30)
        
        print('hydro_weather() done')

    except:
        print('Hydro Weather: not fetched')
        
    
    try:
        wind_cities = {'Mojave': 5373965, 'Tehachapi': 5401297, 
                       'Palm Springs': 5380668, 'Livermore': 5367440,
                       'Fairfield': 5347335}

        wind_weather = {}

        for city, key in wind_cities.items():
            url = 'https://api.openweathermap.org/data/2.5/weather?id={}&appid={}&units=metric'.format(key, weather_key)
            response = requests.get(url)
            print('wind_cities get done')
            city_weather = json.loads(response.text)
            wind_weather[city] = city_weather
            #time.sleep(30)
        
        print('wind_weather() done')    
    
    except:
        print('Wind Weather: not fetched')


    try:
        solar_cities = {'Calexico': 5332698, 'Los Angeles': 1705545, 
                        'Bakersfield': 5325738, 'Rancho Santa Margarita': 5386082, 
                        'Citrus Heights': 5337561, 'Red Bluff': 5570065, 'San Francisco': 1689969}
        
        solar_weather = {}

        for city, key in solar_cities.items():
            url = 'https://api.openweathermap.org/data/2.5/weather?id={}&appid={}&units=metric'.format(key, weather_key)
            response = requests.get(url)
            print('solar_cities get done')
            city_weather = json.loads(response.text)
            solar_weather[city] = city_weather
            #time.sleep(30)
        
        print('solar_weather() done')
    
    except:
        print('Solar Weather: not fetched')
    
    return energy_generation,demand,solar_weather,hydro_weather,wind_weather