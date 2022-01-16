import os
import boto3
import pandas as pd
import json
import glob
from sqlalchemy import create_engine
import mysql.connector
from flatten_json import flatten


class aws_data():

    def __init__(self, filename):
        self.files = glob.glob(filepath+filename)
        

    def combfiles(self):
        dfs = []
        for fp in self.files:
            dfs.append(pd.read_csv(fp))
        concatdf = pd.concat(dfs, ignore_index=True)
        return concatdf

    def flat(self, dataf):
        flatdf = dataf.values()
        print(flatdf)
        dic_flattened = [flatten(d) for d in flatdf]
        flatdf = pd.DataFrame(dic_flattened)
        print(flatdf)
        return flatdf

if __name__ == '__main__':

    s3 = boto3.resource(
        service_name='s3',
        region_name='us-east-1',
        aws_access_key_id='ASIAQXBC2DGI7RFCYX4C',
        aws_secret_access_key='soihDz5UZnjYiEISGpiDG7wbmTAzv8oXv3SjHlmm',
        aws_session_token='FwoGZXIvYXdzEBgaDPTH8Z0JwxIEE9GUXCK5AViM7Ik03ZWHpWBWcTX4Y9tCxeKbfRANNKEoGUyuLjhmrCfEr2p5Grqi+tP90VltQRD8hVpxYiCsl0M9pGTGIpCMk0Bkkh61O3clCyqggF4RyniBe3Omo3dgbHcsfZajYxflVyi3Ds4Sgn0PB9aVEuIghS3V4ZvsGtBhFPIoWRyKGGlgN56jH78DzaFMEEexe7x2KEfFa2oxxaWSIBr1Ns3ldKfRLqfGtpybcM8yG3SbCbKSpAGM7J+1KIScgo4GMi3Qkf/i+ECb0yfECBgUs4x7KNyfAj8qS9aNIGlJREcvNlYTMudUh+1c7QpzFzc=',
    )

    #Select bucket
    my_bucket = s3.Bucket('datalakepartition')
    
    #Download files
    for s3_object in my_bucket.objects.all():
        #Need to split s3_object.key into path and file name, else it will give error file not found.
       path, filename = os.path.split(s3_object.key)
       my_bucket.download_file(s3_object.key, filename)
    
    #Create an engine to the DB
    engine = create_engine(
    'mysql+mysqlconnector://admin:energy2021!@database-1.canx610strnv.us-east-1.rds.amazonaws.com/energy'
    )

    # Local path where S3 Data was downloaded
    filepath = "/Users/aristide/Library/Mobile Documents/com~apple~CloudDocs/01_Studium-IDS/03_Semester/07_DWL03 Data Warehouse and Data Lake Systems 2/10_GroupWork/Data/"
    # Demand

    demand = aws_data('*demand*.csv')
    df_demand = demand.combfiles()
    df_demand = df_demand.rename(columns={"Unnamed: 0": "Date"})
    df_demand.to_sql('demand_dev', con=engine, if_exists='replace')

    energy = aws_data('*energy*.csv')
    df_energy = energy.combfiles()
    df_energy = df_energy.rename(columns={"Unnamed: 0": "Date"})
    df_energy.to_sql('energy_dev', con=engine, if_exists='replace')

    # Json format which needs to be flattened
    solar = aws_data('*solar*.json')
    df = solar.combfiles()
    df_solar = solar.flat(df)
    df_solar.to_sql('solar_dev', con=engine, if_exists='replace')

    hydro = aws_data('*hydro*.json')
    df = hydro.combfiles()
    df_hydro = solar.flat(df)
    df_hydro.to_sql('hydro_dev', con=engine, if_exists='replace')

    wind = aws_data('*wind*.json')
    df = wind.combfiles()
    df_wind = wind.flat(df)
    df_wind.to_sql('wind_dev', con=engine, if_exists='replace')


