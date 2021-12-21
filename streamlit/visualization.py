
import os, sys, json, time

import numpy as np
import pandas as pd
import streamlit as st

from matplotlib import pyplot as plt
from PIL import Image
import altair as alt

from data_fetcher.data_fetcher import DataFetcher

# Get datatime
today_obj = time.strptime("20 December, 2021","%d %B, %Y")
today = time.strftime("%d %B %Y", today_obj)

########## Default settings ##########
st.set_option('deprecation.showPyplotGlobalUse', False)

########## Defs ##########
@st.cache(suppress_st_warning=True)
def data_loader(timestamp):
    if timestamp:
        aws_connection = DataFetcher(
            username = 'admin',
            password = 'energy2021!',
            endpoint = 'database-1.canx610strnv.us-east-1.rds.amazonaws.com',
            database = 'energy',
            port = 3306)
        engine,connection,metadata = aws_connection.connect()
        return {table:pd.read_sql(f"SELECT * FROM {table}", engine) for table in aws_connection.tables}, list(aws_connection.tables)

# Creates a list of values
def create_list(elements):
    # Creating a list of values
    prefix_ul = '<ul>'
    sufix_ul = '</ul>'
    string = ''
    for element in elements:
        string += f"<li>{element}</li>"
    return st.markdown(prefix_ul + string + sufix_ul, unsafe_allow_html=True)

# Creater a footer
def create_footer():
    footer=r"""
    <style>
    a:link , a:visited{
    color: blue;
    background-color: transparent;
    text-decoration: underline;
    }

    a:hover,  a:active {
    color: red;
    background-color: transparent;
    text-decoration: underline;
    }

    .footer {
    position: fixed;
    left: 0;
    bottom: 0;
    width: 100%;
    background-color: white;
    color: black;
    text-align: center;
    }
    </style>
    <div class="footer">
    <p>Developed by Bezos Warehousing & Lakes Inc.<a style='display: block; text-align: center;' href="https://github.com/GStechschulte/energy-data" target="_blank">Gabriel Stechulte | Aristide Guldenschuh | Bernardo Freire Barboza da Cruz</a></p>
    </div>
    """
    return st.markdown(footer,unsafe_allow_html=True)

# Create a pyplot 
def create_plot(x,y,x_label:str,y_label:str,title:str):
    plt.plot(x,y)
    plt.xlabel(x_label)
    plt.ylabel(y_label)
    plt.title(title)
    plt.show()
    return st.pyplot()

# create a overview chart 
def create_overview():
    for key, value in names_dict.items():
        st.header(f'Data Visualization: {value}')
        df_generation = df[key]
        return st.line_chart(df_generation)

# Rolling mean
def rolling(i, feature):
    return lambda x: np.around(x[feature].rolling(i).mean())

########## MAIN ##########
df, tables = data_loader(today)

###### Energy Overview (Balance)
col1, col2, col3 = st.columns(3)
with col2:
    st.image(Image.open('./streamlit/img/logo.jpeg'))
st.header('Energy Dashboard')
st.text('This dashboard is a prototype of a dashboard that will be used to visualize energy data.')
# Rolling mean slider
rolling_mean = st.slider('Rolling mean value:', 1, len(df['generation_dev'][['index']]), 1)#round(max_moving_average/2))

### Energy Generation
col1, col2, col3 = st.columns(3)
with col1: 
    st.subheader('Energy Generation')
    df_generation_dev = (df['generation_dev'])
    df_concat = pd.concat(
            [
                df_generation_dev[['index','hydro_MWh']].rename(columns={'hydro_MWh':'Generation','index':'Date'}).assign(Type='Hydro').assign(Generation = rolling(rolling_mean, 'Generation')),
                df_generation_dev[['index','solar_MWh']].rename(columns={'solar_MWh':'Generation','index':'Date'}).assign(Type='Solar').assign(Generation = rolling(rolling_mean, 'Generation')),
                df_generation_dev[['index','wind_MWh']].rename(columns={'wind_MWh':'Generation','index':'Date'}).assign(Type='Wind').assign(Generation = rolling(rolling_mean, 'Generation')),
            ]
        )
    chart = alt.Chart(df_concat).mark_bar().encode(
        x="Date",
        y='sum(Generation)',
        color="Type")
    st.altair_chart(chart, use_container_width=True)
    st.caption('*****ADD Description of Graph')
        
### Energy Demand
with col2:
    st.subheader('Energy Demand')
    df_demand_dev = (df['demand_dev'][['date_ts','MWh']]
                     .set_index('date_ts')
                     .rename(columns = {'MWh':'Demand', 'date_ts':'index'})
                     .rolling(rolling_mean).mean()
                     )
    st.bar_chart(df_demand_dev)
    st.caption('*****ADD Description of Graph')

### Energy Balance
with col3:    
    st.subheader('Net balance')
    df_delta = (
        (df_generation_dev
            .set_index('index')
            .assign(total = lambda x: x.hydro_MWh + x.solar_MWh + x.wind_MWh)
            .rename(columns={'total':'Generation'})
            )
            .merge(df_demand_dev, left_index = True, right_index = True)
            .assign(balance = lambda x: x.Generation / x.Demand)
            .drop(columns = ['Demand','Generation'])
            .drop(columns = ['hydro_MWh','solar_MWh','wind_MWh'])
            ).rolling(rolling_mean).mean()
    st.bar_chart(df_delta)
    st.caption('*****ADD Description of Graph')

###### Energy Overview by Type
text = 'Energy Overview by Type'
col4, col5, col6 = st.columns(3)
with col4:
    st.subheader(f'{text}: Hydro')
    df_generation_dev = (df['generation_dev']
                         .set_index('index')
                         .drop(columns = ['solar_MWh','wind_MWh']) #['hydro_MWh','solar_MWh','wind_MWh'])
                         .rolling(rolling_mean).mean()
                         )
    st.bar_chart(df_generation_dev)
    st.caption('*****ADD Description of Graph')
    
with col5:
    st.subheader(f'{text}: Solar')
    df_generation_dev = (df['generation_dev']
                         .set_index('index')
                         .drop(columns = ['hydro_MWh','wind_MWh']) #['hydro_MWh','solar_MWh','wind_MWh'])
                         .rolling(rolling_mean).mean()
                         )
    st.bar_chart(df_generation_dev)
    st.caption('*****ADD Description of Graph')
    
with col6:
    st.subheader(f'{text}: Wind')
    df_generation_dev = (df['generation_dev']
                         .set_index('index')
                         .drop(columns = ['hydro_MWh','solar_MWh']) #['hydro_MWh','solar_MWh','wind_MWh'])
                         .rolling(rolling_mean).mean()
                         )
    st.bar_chart(df_generation_dev)    
    st.caption('*****ADD Description of Graph')
    
###### Energy Overview by Type and Location
text = 'Energy Generation by Type and Location'
col7, col8, col9 = st.columns(3) 
with col7:
    st.subheader(f'{text}: Hydro')
    df_hydro_dev = (df['hydro_dev'][['coord_lon','coord_lat']]
                    .rename(columns = {'coord_lon':'lon','coord_lat':'lat'})                    
                    )
    st.map(df_hydro_dev)
    st.caption('*****ADD Description of Graph')
    
with col8:
    st.subheader(f'{text}: Solar')
    df_solar_dev = (df['solar_dev'][['coord_lon','coord_lat']]
                    .rename(columns = {'coord_lon':'lon','coord_lat':'lat'})                    
                    )
    st.map(df_solar_dev)
    st.caption('*****ADD Description of Graph')
    
with col9:
    st.subheader(f'{text}: Wind')
    df_wind_dev = (df['wind_dev'][['coord_lon','coord_lat']]
                    .rename(columns = {'coord_lon':'lon','coord_lat':'lat'})                    
                    )
    st.map(df_wind_dev)
    st.caption('*****ADD Description of Graph')
    
### End of the dashboard
create_footer()