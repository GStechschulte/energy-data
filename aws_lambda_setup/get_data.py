import requests
import json
import pandas as pd
import time
import os

class get_data():

    def __init__(self, energy_key, weather_key):
        self.energy_key = energy_key
        self.weather_key = weather_key
        print('Init done')
    
    def energy_generation(self, start):
        """
        This function. . .
        """

        hydro_url = 'http://api.eia.gov/series/?api_key={}&series_id=EBA.CAL-ALL.NG.WAT.H&start={}'.format(self.energy_key, start)
        solar_url = 'http://api.eia.gov/series/?api_key={}&series_id=EBA.CAL-ALL.NG.SUN.H&start={}'.format(self.energy_key, start)
        wind_url = 'http://api.eia.gov/series/?api_key={}&series_id=EBA.CAL-ALL.NG.WND.H&start={}'.format(self.energy_key, start)

        hydro_response = requests.post(hydro_url).json()
        print('Hydro post done')
        solar_response = requests.post(solar_url).json()
        print('Solar post done')
        wind_response = requests.post(wind_url).json()
        print('Wind post done')

        hydro = dict(hydro_response['series'][0]['data'])
        solar = dict(solar_response['series'][0]['data'])
        wind = dict(wind_response['series'][0]['data'])


        energy_generation = pd.DataFrame.from_dict([hydro, solar, wind]).T

        energy_generation.rename(columns={0: 'hydro_MWh',
                                          1: 'solar_MWh',
                                          2: 'wind_MWh'}, inplace=True)

        energy_generation.index = pd.to_datetime(energy_generation.index, utc=True)

        print('energy_generation() done')
        return energy_generation

    def energy_demand(self, start):
        """
        This function. . .
        """

        url = 'http://api.eia.gov/series/?api_key={}&series_id=EBA.CAL-ALL.D.H&start={}'.format(self.energy_key, start)
        energy = requests.post(url).json()
        print('Energy post done')
        data = dict(energy['series'][0]['data'])

        demand = pd.DataFrame.from_dict(data, orient='index', columns=['demand_MWh'])
        demand.index = pd.to_datetime(demand.index, utc=True)

        print('energy_demand() done')
        return demand


    def hydro_weather(self):

        hydro_cities = {'Yosemite Valley': 7262586, 'Tahoe Vista': 5400943, 
                    'Lassen County': 5566544, 'Eldorado': 5947462}

        hydro_weather = {}

        for city, key in hydro_cities.items():
                url = 'http://api.openweathermap.org/data/2.5/weather?id={}&appid={}&units=metric'.format(key, self.weather_key)
                response = requests.get(url)
                print('hydro_cities get done')
                city_weather = json.loads(response.text)
                hydro_weather[city] = city_weather
                #time.sleep(30)
        
        print('hydro_weather() done')
        return hydro_weather
    
    def wind_weather(self):

        wind_cities = {'Mojave': 5373965, 'Tehachapi': 5401297, 
                       'Palm Springs': 5380668, 'Livermore': 5367440,
                       'Fairfield': 5347335}

        wind_weather = {}

        for city, key in wind_cities.items():
            url = 'http://api.openweathermap.org/data/2.5/weather?id={}&appid={}&units=metric'.format(key, self.weather_key)
            response = requests.get(url)
            print('wind_cities get done')
            city_weather = json.loads(response.text)
            wind_weather[city] = city_weather
            #time.sleep(30)
        
        print('wind_weather() done')
        return wind_weather
    
    def solar_weather(self):

        solar_cities = {'Calexico': 5332698, 'Los Angeles': 1705545, 
                        'Bakersfield': 5325738, 'Rancho Santa Margarita': 5386082, 
                        'Citrus Heights': 5337561, 'Red Bluff': 5570065, 'San Francisco': 1689969}
        
        solar_weather = {}

        for city, key in solar_cities.items():
            url = 'http://api.openweathermap.org/data/2.5/weather?id={}&appid={}&units=metric'.format(key, self.weather_key)
            response = requests.get(url)
            print('solar_cities get done')
            city_weather = json.loads(response.text)
            solar_weather[city] = city_weather
            #time.sleep(30)
        
        print('solar_weather() done')
        return solar_weather