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


def save_to_sqlite(data, file_name):
    conn = sqlite3.connect(file_name)
    data.to_sql('data', conn, if_exists='replace', index=False)
    conn.close()
    print('Data saved to sqllite file: {}'.format(file_name))

def data_pipeline(first_url, second_url, ev_infra_file, fuel_prices_file, data_file_type, database_type, data_path):

    # run the pipeline for first and second data respectively

    download_data(first_url, data_path, ev_infra_file + data_file_type)
    ev_infra_data = read_csv_file(os.path.join(data_path, ev_infra_file + data_file_type))
    save_to_sqlite(ev_infra_data, os.path.join(data_path, ev_infra_file + database_type))

    download_data(second_url, data_path, fuel_prices_file + data_file_type)
    fuel_prices_data = read_csv_file(os.path.join(data_path, fuel_prices_file + data_file_type), sep=';')
    save_to_sqlite(fuel_prices_data, os.path.join(data_path, fuel_prices_file + database_type))

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
