
import requests
import json
import pandas as pd
import time
import os
from datetime import datetime #
from io import StringIO #
import boto3

energy_key = os.environ.get('energy_key','it didnt work')
weather_key = os.environ.get('weather_key','it didnt work')

def lambda_handler(event, context):

    try:
        hydro_url = 'https://api.eia.gov/series/?api_key={}&series_id=EBA.CAL-ALL.NG.WAT.H&start={}'.format(energy_key, '20191018T07Z')
        solar_url = 'https://api.eia.gov/series/?api_key={}&series_id=EBA.CAL-ALL.NG.SUN.H&start={}'.format(energy_key, '20191018T07Z')
        wind_url = 'https://api.eia.gov/series/?api_key={}&series_id=EBA.CAL-ALL.NG.WND.H&start={}'.format(energy_key, '20191018T07Z')
       
        hydro_response = requests.get(hydro_url).json()
        solar_response = requests.get(solar_url).json()
        wind_response = requests.get(wind_url).json()
        print('--------- API connection established ---------')
    except:
        print('--------- Loading not successfull ---------')
        
    try:
        hydro = dict(hydro_response['series'][0]['data'])
        solar = dict(solar_response['series'][0]['data'])
        wind = dict(wind_response['series'][0]['data'])

        energy_generation = pd.DataFrame.from_dict([hydro, solar, wind]).T
    
        energy_generation.rename(columns={0: 'hydro_MWh',
                                          1: 'solar_MWh',
                                          2: 'wind_MWh'}, inplace=True)
    
        energy_generation.index = pd.to_datetime(energy_generation.index, utc=True)
    except:
        print('--------- Energy Generation: DataFrame generation didnt work ---------')
    
    try:
        url = 'https://api.eia.gov/series/?api_key={}&series_id=EBA.CAL-ALL.D.H&start={}'.format(energy_key, '20191018T07Z')
        energy = requests.post(url).json()
        data = dict(energy['series'][0]['data'])
    
        demand = pd.DataFrame.from_dict(data, orient='index', columns=['demand_MWh'])
        demand.index = pd.to_datetime(demand.index, utc=True)
    
    except:
        print('--------- Energy Demand: DataFrame generation didnt work ---------')


    try:
        hydro_cities = {'Yosemite Valley': 7262586, 'Tahoe Vista': 5400943, 
                    'Lassen County': 5566544, 'Eldorado': 5947462}

        hydro_weather = {}

        for city, key in hydro_cities.items():
                url = 'https://api.openweathermap.org/data/2.5/weather?id={}&appid={}&units=metric'.format(key, weather_key)
                response = requests.get(url)
                city_weather = json.loads(response.text)
                hydro_weather[city] = city_weather
                #time.sleep(30)

    except:
        print('--------- Hydro Weather: not fetched ---------')
        
    
    try:
        wind_cities = {'Mojave': 5373965, 'Tehachapi': 5401297, 
                       'Palm Springs': 5380668, 'Livermore': 5367440,
                       'Fairfield': 5347335}

        wind_weather = {}

        for city, key in wind_cities.items():
            url = 'https://api.openweathermap.org/data/2.5/weather?id={}&appid={}&units=metric'.format(key, weather_key)
            response = requests.get(url)
            city_weather = json.loads(response.text)
            wind_weather[city] = city_weather
            #time.sleep(30)
    
    except:
        print('--------- Wind Weather: not fetched ---------')


    try:
        solar_cities = {'Calexico': 5332698, 'Los Angeles': 1705545, 
                        'Bakersfield': 5325738, 'Rancho Santa Margarita': 5386082, 
                        'Citrus Heights': 5337561, 'Red Bluff': 5570065, 'San Francisco': 1689969}
        
        solar_weather = {}

        for city, key in solar_cities.items():
            url = 'https://api.openweathermap.org/data/2.5/weather?id={}&appid={}&units=metric'.format(key, weather_key)
            response = requests.get(url)
            city_weather = json.loads(response.text)
            solar_weather[city] = city_weather
            #time.sleep(30)
            
    except:
        print('--------- Solar Weather: not fetched ---------')
    
    try:
        
        now = datetime.now() # current date and time
        time = now.strftime("%Y%m%d_%H%M")
        print(f'---------{time} Upload to S3 datalake ---------')
        
        bucket = 'datalakepartition' # already created on S3
        s3_resource = boto3.resource(
            's3',
            aws_access_key_id='ASIAZXXMZED4ZKN5DCGR',
            aws_secret_access_key='AzrSgVOtq1ruy5jHWWLLqbVRD3fGbqafU7z81fAd',
            aws_session_token='FwoGZXIvYXdzEGkaDMueLIVJA15lD81EMCLNATaFzpeFoSE/iBKWOyB0SI56V15V/ewiHLOxIpaDioupDf3lz+nIFm/vwuPNfu+/BtHa7IxQl174SPdUWwvg86rh1JYFSZBAhS6UN3aGkF3sQAFocqVcUjj3h9bCw86/Vj0K+4W5iCl+u7F/q0laNfZve1awLOYVYSZCZCmJw2f1FwdvdBlAsm8GOnipp62C3l06ZkIZTNE8oIhf6uhsoEnsq8JUyVoUDLolFFL2QGI/rf7jn7aArjQJin3nett34+UrRkjFjnb2XVWGuS8o2fT6iwYyLWTrpopb6F/wepXcbhoZ8T5+XsC9l1aeWLe04fV2tKkl/TA/KPfyr7roAbWCOg==',
        )
        csv_buffer = StringIO()
        energy_generation.to_csv(csv_buffer)
        s3_resource.Object(bucket, f'{time}_energy_generation.csv').put(Body=csv_buffer.getvalue())
        print('1/5 uploaded (Energy Generation)')
        
        csv_buffer = StringIO()
        demand.to_csv(csv_buffer)
        s3_resource.Object(bucket, f'{time}_demand.csv').put(Body=csv_buffer.getvalue())
        print('2/5 uploaded (Energy Demand)')

        json_buffer = StringIO()
        json.dump(solar_weather, json_buffer)
        s3_resource.Object(bucket, f'{time}_solar_weather.json').put(Body=json_buffer.getvalue())
        print('3/5 uploaded (Solar Weather)')
        
        json_buffer = StringIO()
        json.dump(hydro_weather, json_buffer)
        s3_resource.Object(bucket, f'{time}_hydro_weather.json').put(Body=json_buffer.getvalue())
        print('4/5 uploaded (Hydro Weather)')
        
        json_buffer = StringIO()
        json.dump(wind_weather, json_buffer)
        s3_resource.Object(bucket, f'{time}_wind_weather.json').put(Body=json_buffer.getvalue())
        print('5/5 uploaded (Wind Weather)')
    except:
        print('--------- Data not uploaded to S3 ---------')

#######################################################3

# Local Module 
#energy_key = '2f13235137d441c33ec363a5d85a86cb'
#weather_key = '9ea3a7cd842bc24bd99785092ce3342e'

#lambda_handler(event=True, context=True)
#print('done')
