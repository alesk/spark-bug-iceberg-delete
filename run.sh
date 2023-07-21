!#/usr/bin/bash
set -x -e

rm -rf .venv
python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install pyspark==3.4.1
python3 min_reproduce.py

