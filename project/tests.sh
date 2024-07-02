#!/bin/bash
echo "Deleting old test data"
rm -r ./data/ev-infra.csv
rm -r ./data/ev-infra.sqlite
rm -r ./data/fuel-prices.csv
rm -r ./data/fuel-prices.sqlite
echo "Running tests"
python ./project/tests.py