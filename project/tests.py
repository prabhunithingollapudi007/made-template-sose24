import os
from pipeline import data_pipeline

# run the pipeline and test if .sqlite files are created

def test_data_pipeline():
    # information about the data
    first_url = 'https://www.data.gouv.fr/fr/datasets/r/2729b192-40ab-4454-904d-735084dca3a3'
    second_url = 'https://data.economie.gouv.fr/api/explore/v2.1/catalog/datasets/prix-carburants-quotidien/exports/csv'

    ev_infra_file = 'ev-infra'
    fuel_prices_file = 'fuel-prices'

    data_file_type = '.csv'
    database_type = '.sqlite'

    data_path = 'data'
    print("Running the pipeline to download the data and save it to sqlite files")
    data_pipeline(first_url=first_url, second_url=second_url, ev_infra_file=ev_infra_file, fuel_prices_file=fuel_prices_file, data_file_type=data_file_type, database_type=database_type, data_path=data_path)

    # check if the files are created
    print("Checking if the .sqlite files are created")
    assert os.path.exists(os.path.join(data_path, ev_infra_file + database_type))
    assert os.path.exists(os.path.join(data_path, fuel_prices_file + database_type))

    print("Test passed successfully")

test_data_pipeline()