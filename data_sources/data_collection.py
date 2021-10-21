import requests
import json
import pandas as pd
import time

class get_data():

    def __init__(self, energy_key, weather_key):
        self.energy_key = energy_key
        self.weather_key = weather_key

    def energy_generation(self, start):
        """
        This function. . .
        """

        hydro_url = 'http://api.eia.gov/series/?api_key={}&series_id=EBA.CAL-ALL.NG.WAT.H&start={}'.format(self.energy_key, start)
        solar_url = 'http://api.eia.gov/series/?api_key={}&series_id=EBA.CAL-ALL.NG.SUN.H&start={}'.format(self.energy_key, start)
        wind_url = 'http://api.eia.gov/series/?api_key={}&series_id=EBA.CAL-ALL.NG.WND.H&start={}'.format(self.energy_key, start)

        hydro_response = requests.post(hydro_url).json()
        solar_response = requests.post(solar_url).json()
        wind_response = requests.post(wind_url).json()

        hydro = dict(hydro_response['series'][0]['data'])
        solar = dict(solar_response['series'][0]['data'])
        wind = dict(wind_response['series'][0]['data'])


        energy_generation = pd.DataFrame.from_dict([hydro, solar, wind]).T

        energy_generation.rename(columns={0: 'hydro_MWh',
                                          1: 'solar_MWh',
                                          2: 'wind_MWh'}, inplace=True)

        energy_generation.index = pd.to_datetime(energy_generation.index, utc=True)

        return energy_generation

    def energy_demand(self, start):
        """
        This function. . .
        """

        url = 'http://api.eia.gov/series/?api_key={}&series_id=EBA.CAL-ALL.D.H&start={}'.format(self.energy_key, start)
        energy = requests.post(url).json()
        data = dict(energy['series'][0]['data'])

        demand = pd.DataFrame.from_dict(data, orient='index', columns=['demand_MWh'])
        demand.index = pd.to_datetime(demand.index, utc=True)

        return demand


    def hydro_weather(self):

        hydro_cities = {'Yosemite Valley': 7262586, 'Tahoe Vista': 5400943, 
                    'Lassen County': 5566544, 'Eldorado': 5947462}

        hydro_weather = {}

        for city, key in hydro_cities.items():
                url = 'http://api.openweathermap.org/data/2.5/weather?id={}&appid={}&units=metric'.format(key, self.weather_key)
                response = requests.get(url)
                city_weather = json.loads(response.text)
                hydro_weather[city] = city_weather
                #time.sleep(30)
        
        return hydro_weather
    
    def wind_weather(self):

        wind_cities = {'Mojave': 5373965, 'Tehachapi': 5401297, 
                       'Palm Springs': 5380668, 'Livermore': 5367440,
                       'Fairfield': 5347335}

        wind_weather = {}

        for city, key in wind_cities.items():
            url = 'http://api.openweathermap.org/data/2.5/weather?id={}&appid={}&units=metric'.format(key, self.weather_key)
            response = requests.get(url)
            city_weather = json.loads(response.text)
            wind_weather[city] = city_weather
            #time.sleep(30)
        
        return wind_weather
    
    def solar_weather(self):

        solar_cities = {'Calexico': 5332698, 'Los Angeles': 1705545, 
                        'Bakersfield': 5325738, 'Rancho Santa Margarita': 5386082, 
                        'Citrus Heights': 5337561, 'Red Bluff': 5570065, 'San Francisco': 1689969}
        
        solar_weather = {}

        for city, key in solar_cities.items():
            url = 'http://api.openweathermap.org/data/2.5/weather?id={}&appid={}&units=metric'.format(key, self.weather_key)
            response = requests.get(url)
            city_weather = json.loads(response.text)
            solar_weather[city] = city_weather
            #time.sleep(30)
        
        return solar_weather
        
        


if __name__ == '__main__':

    load = get_data('2f13235137d441c33ec363a5d85a86cb', '9ea3a7cd842bc24bd99785092ce3342e')
    energy_generation = load.energy_generation('20191018T07Z')
    energy_demand = load.energy_demand('20191018T07Z')

    print(energy_generation.head(), '\n')
    print(25*'-', '\n')
    print(energy_demand.head())

    hydro = load.hydro_weather()
    wind = load.wind_weather()
    solar = load.solar_weather()

    print(hydro, '\n')
    print(25*'-', '\n')
    print(wind, '\n')
    print(25*'-', '\n')
    print(solar)

    