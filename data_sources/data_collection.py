import requests
import json
import pandas as pd

class get_data():

    def __init__(self, energy_key):
        self.energy_key = energy_key
        #self.weather_key = weather_key

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

    #def meteorology(self):
    #    """
    #    This function. . .
    #    """

if __name__ == '__main__':

    load = get_data('2f13235137d441c33ec363a5d85a86cb')
    energy_generation = load.energy_generation('20191018T07Z')
    energy_demand = load.energy_demand('20191018T07Z')

    print(energy_generation.head(), '\n')
    print(25*'-', '\n')
    print(energy_demand.head()) 
    