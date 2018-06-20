import pandas as pd
from sqlalchemy import create_engine

CONN_POSTGRES = 'postgresql://postgres:postgres@localhost:5432/db'

def load_parking_from_csv(file_path):
    df = pd.read_csv(file_path, sep=';', encoding='windows-1251')

    #keep needed columns
    df = df[['ID', 'Name', 'PlaceID', 'global_id', 'AdmArea', 'District', 'CoordinateCenters', 'Coordinates']]
    #clean and process data
    df['geoData'] = df['Coordinates'].str.replace('|', ',')

    #load to postgres db
    engine = create_engine(CONN_POSTGRES,echo=True)
    df.to_sql('parking', engine, schema='mdata', if_exists='append', index=False)

if __name__ == '__main__':
    load_parking_from_csv('data-6079-2018-05-11.csv')