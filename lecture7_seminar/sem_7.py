import pandas as pd
from sqlalchemy import inspect,create_engine
from dateutil.relativedelta import relativedelta
from datetime import datetime
from pandas.io import sql

df = pd.read_excel("/Users/Андрей/Desktop/git_education/ETL_seminar/seminar_7/s4_2.xlsx")

con = create_engine("mysql://Airflow:1@localhost/spark")
df["Долг"] = df["Платеж по основному долгу"].cumsum()
df["Проценты"] = df["Платеж по процентам"].cumsum()
df.to_sql("Платежи", con, schema="spark", if_exists="replace", index=False)

print(df)