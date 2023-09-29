import pandas as pd
import mysql.connector
from sqlalchemy import create_engine


playlist = pd.read_csv('/etl_airflow/data/csv/playlists.csv', index_col=0)

engine = create_engine('mysql+mysqlconnector://admin:admin123@localhost:3306/spotify', echo=False)

playlist.to_sql('featured_playlists', con=engine, if_exists='append', index=False)