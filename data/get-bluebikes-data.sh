#!/bin/bash
set -euo pipefail
wget https://s3.amazonaws.com/hubway-data/20{16,17,18,19,20,21,22}{01,02,03,04,05,06,07,08,09,10,11,12}-hubway-tripdata.zip
wget https://s3.amazonaws.com/hubway-data/current_bluebikes_stations.csv
