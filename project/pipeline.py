# use this script to download the csv files from the sources

import requests
import os
import pandas as pd
import sqlite3

data_path = 'data'

# create a directory to store the data
os.makedirs(data_path, exist_ok=True)

# download the data
first_url = 'https://www.data.gouv.fr/fr/datasets/r/2729b192-40ab-4454-904d-735084dca3a3'
second_url = 'https://data.economie.gouv.fr/api/explore/v2.1/catalog/datasets/prix-carburants-quotidien/exports/csv'

file_type = '.csv'
db_type = '.sqlite'
first_file = 'ev-infra' + file_type
second_file = 'fuel-prices' + file_type

print('Downloading data...')

# download the first file
response = requests.get(first_url)
with open(os.path.join(data_path, first_file), 'wb') as f:
    f.write(response.content)

# download the second file
response = requests.get(second_url)
with open(os.path.join(data_path, second_file), 'wb') as f:
    f.write(response.content)

print('Data downloaded and saved successfully to directory: {}'.format(data_path))


# save the csv files to sqlite database
print('Saving data to sqlite database...')

# load the data
data1 = pd.read_csv(os.path.join(data_path, first_file), sep=',')
data2 = pd.read_csv(os.path.join(data_path, second_file), sep=';')

# create a connection to the database for data1 and save it
first_file = 'ev-infra' + db_type
second_file = 'fuel-prices' + db_type

conn1 = sqlite3.connect(os.path.join(data_path, first_file))
data1.to_sql('data1', conn1, if_exists='replace', index=False)
conn1.close()

# create a connection to the database for data2 and save it
conn2 = sqlite3.connect(os.path.join(data_path, second_file))
data2.to_sql('data2', conn2, if_exists='replace', index=False)
conn2.close()

print('Data saved to sqlite database successfully')