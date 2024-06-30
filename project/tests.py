import os, pandas as pd
from pipeline import data_pipeline, select_columns, rename_columns

# run the pipeline and test if .sqlite files are created

def test_data_pipeline():
    print("============= Testing the data pipeline =============")
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

    print("============= Data pipeline test passed successfully =============")

def test_select_columns():
    # test the select_columns function
    print("============= Testing the select_columns and rename_columns functions =============")
    data = pd.DataFrame({'a': [1, 2, 3], 'b': [4, 5, 6], 'c': [7, 8, 9]})
    columns = ['a', 'c']
    result = select_columns(data, columns)
    assert result.equals(pd.DataFrame({'a': [1, 2, 3], 'c': [7, 8, 9]}))
    print("============= Select columns test passed successfully =============")

def test_rename_columns():
    # test the rename_columns function
    print("============= Testing the rename_columns function =============")
    data = pd.DataFrame({'a': [1, 2, 3], 'b': [4, 5, 6], 'c': [7, 8, 9]})
    columns = {'a': 'A', 'b': 'B', 'c': 'C'}
    result = rename_columns(data, columns)
    assert result.equals(pd.DataFrame({'A': [1, 2, 3], 'B': [4, 5, 6], 'C': [7, 8, 9]}))
    print("============= Rename columns test passed successfully =============")

if __name__ == '__main__':
    test_select_columns()
    test_rename_columns()
    test_data_pipeline()