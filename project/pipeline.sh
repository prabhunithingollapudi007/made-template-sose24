#!/bin/bash
rm -r data/ev-infra.csv
rm -r data/ev-infra.sqlite
rm -r data/fuel-prices.csv
rm -r data/fuel-prices.sqlite
python project/pipeline.py