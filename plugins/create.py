import sys
import os 
sys.path.append("C\\wsl$\\Ubuntu\\home\\gress\\airflow")
sys.path.insert(0, os.getcwd())
from plugins.config import config
import sqlalchemy
import psycopg2


def writeIntoDB(DataObject):
    connection = None
    try:

         # Load
        engine = sqlalchemy.create_engine('postgresql+psycopg2://postgres:p@localhost/airflow_database')

        # read connection parameters
        params = config()

        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
       
        connection = psycopg2.connect(**params)
        cursor = connection.cursor()
        song_df = DataObject

        try:
             song_df.to_sql("spotify_data", engine, index=False, if_exists='append')
        except (Exception) as error:
             print(error)

    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgreSQL", error)
    finally:
        if connection:
            cursor.close()
            connection.commit()
            print("Data successful inserted into PostgreSQL")
            connection.close()
            print("PostgreSQL connection is closed")