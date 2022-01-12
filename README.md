[![Imports: isort](https://img.shields.io/badge/%20imports-isort-%231674b1?style=flat&labelColor=ef8336)](https://pycqa.github.io/isort/)

# 2022 Data processing

## Data Inputs

United States Register of Introduced and Invasive Species (US-RIIS)
https://doi.org/10.5066/P95XL09Q

## Test USGS input files

python3 test/test_RIIS.py
python3 test/test_taxonomy.py

# Project setup

## Python Dependencies
for development virtual environment and production build
  * [requirements.txt](requirements.txt)

Use a Python virtual environment, by installing and activating
```commandline
python3 -m venv venv
. venv/bin/activate
pip3 install <python dependencies>
```

# Pre-commit

* Install pre-commit (listed in requirements.txt)
* Execute ```pre-commit install``` to install git hooks in your .git/ directory.
