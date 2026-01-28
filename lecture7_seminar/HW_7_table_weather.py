import pandas as pd
from sqlalchemy import inspect,create_engine
from dateutil.relativedelta import relativedelta
from datetime import datetime, timedelta
from pandas.io import sql
import pendulum
import requests

# Функция для получения температуры из API OpenWeatherMap
def get_temperature(api_key):
    url = 'https://api.openweathermap.org/data/2.5/weather?q=Vladivostok&appid=7f1baa1d74ab9d393b97eab4b600ef44' 
    response = requests.get(url)
    data = response.json()
    temperature = data['main']['temp']
    return temperature

# Функция для записи температуры и текущего времени в базу данных
def save_temperature_to_db(temperature):
    engine = create_engine("mysql://Airflow:1@localhost/spark")
    metadata = MetaData(bind=engine)
    Temperature_Vladivostok = Table('Temperature_Vladivostok', metadata, autoload=True)

    with engine.connect() as connection:
        ins = Temperature_Vladivostok.insert().values(temperature=temperature)
        connection.execute(ins)

# Получаем API ключ OpenWeatherMap
api_key = '7f1baa1d74ab9d393b97eab4b600ef44'

# Получаем температуру и записываем ее в базу данных
temperature = get_temperature(api_key)
save_temperature_to_db(temperature)