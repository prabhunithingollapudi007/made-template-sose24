# use this script to download the csv files from the sources

import requests
import os
import pandas as pd
import sqlite3


def download_data(url, data_path, file_name):
    print('Downloading data from: {}'.format(url) + ' to file: {}'.format(file_name))
    if not os.path.exists(data_path):
        os.makedirs(data_path)
        print('Directory created: {}'.format(data_path))
    response = requests.get(url)
    with open(os.path.join(data_path, file_name), 'wb') as f:
        f.write(response.content)
    print('Data downloaded successfully to file: {}'.format(file_name) + ' in directory: {}'.format(data_path))


def read_csv_file(file_path, sep=','):
    print('Reading data from file: {}'.format(file_path))
    return pd.read_csv(file_path, sep=sep)

def select_columns(data, columns):
    print('Selecting columns: {}'.format(columns))
    return data[columns]

def rename_columns(data, columns):
    print('Renaming columns: {}'.format(columns))
    return data.rename(columns=columns)

def drop_na(data):
    print('Dropping NA values')
    return data.dropna()

def convert_to_datetime(data, columns):
    for column in columns:
        print('Converting column: {} to datetime'.format(column))
        # trim the date to the first 10 characters to format dd-mm-yy
        data[column] = data[column].str[:10]
        data[column] = pd.to_datetime(data[column], format='%Y-%m-%d')
    return data

def save_to_sqlite(data, file_name, table_name='data'):
    conn = sqlite3.connect(file_name)
    data.to_sql(table_name, conn, if_exists='replace', index=False)
    conn.close()
    print('Data saved to sqllite file: {}'.format(file_name))

def data_pipeline(first_url, second_url, ev_infra_file, fuel_prices_file, data_file_type, database_type, data_path):

    # run the pipeline for first and second data respectively

    download_data(first_url, data_path, ev_infra_file + data_file_type)
    ev_infra_data = read_csv_file(os.path.join(data_path, ev_infra_file + data_file_type))
    ev_infra_data = select_columns(ev_infra_data, ['nom_station', 'id_pdc_itinerance', 'date_mise_en_service', 'date_maj', 'last_modified', 'created_at'])
    ev_column_map = {'nom_station': 'station_name', 'id_pdc_itinerance': 'station_id', 'date_mise_en_service': 'station_service_start_date', 'date_maj': 'date_modified'}
    ev_infra_data = rename_columns(ev_infra_data, ev_column_map)
    ev_infra_data = drop_na(ev_infra_data)
    ev_infra_data = convert_to_datetime(ev_infra_data, ['station_service_start_date', 'date_modified', 'last_modified', 'created_at'])
    save_to_sqlite(ev_infra_data, os.path.join(data_path, ev_infra_file + database_type), table_name='ev')

    download_data(second_url, data_path, fuel_prices_file + data_file_type)
    fuel_prices_data = read_csv_file(os.path.join(data_path, fuel_prices_file + data_file_type), sep=';')
    fuel_prices_data = select_columns(fuel_prices_data, ['prix_maj', 'prix_id', 'prix_valeur', 'prix_nom'])
    fuel_column_map = {'prix_maj': 'date_modified', 'prix_id': 'price_id', 'prix_valeur': 'price_value', 'prix_nom': 'price_name'}
    fuel_prices_data = rename_columns(fuel_prices_data, fuel_column_map)
    fuel_prices_data = drop_na(fuel_prices_data)
    fuel_prices_data = convert_to_datetime(fuel_prices_data, ['date_modified'])
    save_to_sqlite(fuel_prices_data, os.path.join(data_path, fuel_prices_file + database_type), table_name='fuel')

if __name__ == '__main__':
    # information about the data
    first_url = 'https://www.data.gouv.fr/fr/datasets/r/2729b192-40ab-4454-904d-735084dca3a3'
    second_url = 'https://data.economie.gouv.fr/api/explore/v2.1/catalog/datasets/prix-carburants-quotidien/exports/csv'

    ev_infra_file = 'ev-infra'
    fuel_prices_file = 'fuel-prices'

    data_file_type = '.csv'
    database_type = '.sqlite'

    data_path = 'data'

    data_pipeline(first_url=first_url, second_url=second_url, ev_infra_file=ev_infra_file, fuel_prices_file=fuel_prices_file, data_file_type=data_file_type, database_type=database_type, data_path=data_path)
