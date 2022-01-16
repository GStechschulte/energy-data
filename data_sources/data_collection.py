import requests
import json
import pandas as pd
import typing_extensions
import sys
from sqlalchemy import create_engine
import mysql.connector
from flatten_json import flatten

class get_data():

    def __init__(self, energy_key, weather_key):
        self.energy_key = energy_key
        self.weather_key = weather_key

    def energy_generation(self, start):
        """
        This function fetches hydro, solar, and wind energy production from the 
        state of California and processes the JSON format into a pandas dataframe
        with respective columns and data types.
        """

        hydro_url = 'http://api.eia.gov/series/?api_key={}&series_id=EBA.CAL-ALL.NG.WAT.H&start={}'.format(
            self.energy_key, start)
        solar_url = 'http://api.eia.gov/series/?api_key={}&series_id=EBA.CAL-ALL.NG.SUN.H&start={}'.format(
            self.energy_key, start)
        wind_url = 'http://api.eia.gov/series/?api_key={}&series_id=EBA.CAL-ALL.NG.WND.H&start={}'.format(
            self.energy_key, start)

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
        This function fetches the energy demand from the 
        state of California and processes the JSON format into a pandas dataframe
        with respective columns and data types.
        """

        url = 'http://api.eia.gov/series/?api_key={}&series_id=EBA.CAL-ALL.D.H&start={}'.format(self.energy_key, start)
        energy = requests.post(url).json()
        data = dict(energy['series'][0]['data'])

        demand = pd.DataFrame.from_dict(data, orient='index', columns=['MWh'])
        demand.index = pd.to_datetime(demand.index, utc=True)
        demand.reset_index(inplace=True)
        demand.rename(columns={'index': 'date_ts'}, inplace=True)

        return demand

    def hydro_weather(self):
        """
        This function fetches the weather components from the 
        areas of California with the largest hydro energy production
        """

        hydro_cities = {'Yosemite Valley': 7262586, 'Tahoe Vista': 5400943, 
                    'Lassen County': 5566544, 'Eldorado': 5947462}

        hydro_weather = {}

        for city, key in hydro_cities.items():
                url = 'http://api.openweathermap.org/data/2.5/weather?id={}&appid={}&units=metric'.format(key, self.weather_key)
                response = requests.get(url)
                city_weather = json.loads(response.text)
                hydro_weather[city] = city_weather
        
        return hydro_weather
    
    def wind_weather(self):
        """
        This function fetches the weather components from the 
        areas of California with the largest wind energy production
        """

        wind_cities = {'Mojave': 5373965, 'Tehachapi': 5401297, 
                       'Palm Springs': 5380668, 'Livermore': 5367440,
                       'Fairfield': 5347335}

        wind_weather = {}

        for city, key in wind_cities.items():
            url = 'http://api.openweathermap.org/data/2.5/weather?id={}&appid={}&units=metric'.format(key, self.weather_key)
            response = requests.get(url)
            city_weather = json.loads(response.text)
            wind_weather[city] = city_weather
        
        return wind_weather
    
    def solar_weather(self):
        """
        This function fetches the weather components from the 
        areas of California with the largest solar energy production
        """

        solar_cities = {'Calexico': 5332698, 'Los Angeles': 1705545, 
                        'Bakersfield': 5325738, 'Rancho Santa Margarita': 5386082, 
                        'Citrus Heights': 5337561, 'Red Bluff': 5570065, 'San Francisco': 1689969}
        
        solar_weather = {}

        for city, key in solar_cities.items():
            url = 'http://api.openweathermap.org/data/2.5/weather?id={}&appid={}&units=metric'.format(key, self.weather_key)
            response = requests.get(url)
            city_weather = json.loads(response.text)
            solar_weather[city] = city_weather
        
        return solar_weather
        

if __name__ == '__main__':

    # Create an engine to the DB
    #engine = create_engine(
    #    'mysql+mysqlconnector://admin:energy2021!@database-1.canx610strnv.us-east-1.rds.amazonaws.com/energy'
    #    )

    # Gabe's Local DB
    #engine = create_engine(
    #    'postgresql+psycopg2://postgres:SwissAmerican2020@localhost/postgres'
    #    )

    # Fetch energy demand and generation data
    load = get_data('cfc655e7e0bb8f38367dc611a0da9409', '9ea3a7cd842bc24bd99785092ce3342e')
    energy_generation = load.energy_generation('20211001T07Z')
    energy_demand = load.energy_demand('20211001T07Z')

    print(energy_generation.head(), '\n')
    print(25*'-', '\n')
    print(energy_demand.head())
    
    # Fetch weather data
    hydro = load.hydro_weather()
    wind = load.wind_weather()
    solar = load.solar_weather()

    print(hydro, '\n')
    print(25*'-', '\n')
    print(wind, '\n')
    print(25*'-', '\n')
    print(solar)

    # hydro
    hydro = hydro.values()
    dic_flattened = [flatten(d) for d in hydro]
    df_hydro = pd.DataFrame(dic_flattened)
    df_hydro.to_sql('hydro_dev', con=engine, if_exists='replace')
    query = """select * from hydro_dev"""
    hydro_data = pd.read_sql_query(query, con=engine)
    print("---------Hydro Data-------------")
    print(hydro_data)

    # solar
    solar = solar.values()
    dic_flattened = [flatten(d) for d in solar]
    df_solar = pd.DataFrame(dic_flattened)
    df_solar.to_sql('solar_dev', con=engine, if_exists='replace')
    query = """select * from solar_dev"""
    solar_data = pd.read_sql_query(query, con=engine)
    print("---------Solar Data-------------")
    print(solar_data)
    
    # Wind
    wind = wind.values()
    dic_flattened = [flatten(d) for d in wind]
    df_wind = pd.DataFrame(dic_flattened)
    df_wind.to_sql('wind_dev', con=engine, if_exists='replace')
    query = """select * from wind_dev"""
    print("---------Wind Data-------------")
    wind_data = pd.read_sql_query(query, con=engine)


    # Load demand and energy generation data into DB table
    energy_demand.to_sql('demand_dev', con=engine, if_exists='replace')    
    energy_generation.to_sql('energy_dev', con=engine, if_exists='replace')