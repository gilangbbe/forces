#!/bin/bash

# Update and install system dependencies
sudo apt-get update -y
sudo apt-get install -y python3-pip python3-venv

# Upgrade pip
pip install --upgrade pip

# Install Python dependencies
pip install datetime
pip install colorama
pip install psycopg2-binary
pip install python-dotenv
pip install flask
pip install logging
pip install pandas
pip install requests
pip install openpyxl
pip install simplekml
pip install fastkml
pip install lxml
pip install shapely
pip install cssselect

# Additional packages that might be needed
pip install fnmatch
pip install zipfile

# Install any necessary tools for spatial data handling
sudo apt-get install -y libproj-dev proj-data proj-bin
pip install pyproj

echo "All dependencies have been installed"
