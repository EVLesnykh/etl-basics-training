import pandas as pd
from sqlalchemy import inspect,create_engine
from dateutil.relativedelta import relativedelta
from datetime import datetime
from pandas.io import sql

df = pd.read_excel("/home/ekaterina/s4_2.xlsx")

print(df)

con = create_engine("mysql://Airflow:1@localhost/spark")
df["Долг"] = df["Платеж по основному долгу"].cumsum()
df["Проценты"] = df["Платеж по процентам"].cumsum()
df.to_sql("Платежи", con, schema="spark", if_exists="replace", index=False)