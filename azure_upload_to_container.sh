#!/bin/bash
# 
# This script stores the data downloaded in an Azure storage account. It assumes that
# azure client is installed (instructions here: https://azure.microsoft.com/en-us/documentation/articles/xplat-cli-install/)
# To install in Linux:
# $ sudo apt-get install npm
# $ sudo npm install azure-cli -g
# $ sudo ln -s /usr/bin/nodejs /usr/bin/node

# The next step is to login. Type: 
# $ azure login
# you will be asked to go to a web page and add a code

# export AZURE_STORAGE_ACCOUNT=<storage_account_name>
export AZURE_STORAGE_ACCOUNT=hoaphumanoidstorage2

# export AZURE_STORAGE_ACCESS_KEY=<storage_account_key>
export AZURE_STORAGE_ACCESS_KEY=xNMuv+mc1bLQggDvd4ZQaBbgtk4A431FI6e6CBcpuRU4lqZ6qlx6qTDXSM0iVvx/ibDV89nC7LHrImCGmHnfNQ==


#export container_name=<container_name>
export container_name=nyctaxihive #example of name

echo "Creating the container..."
azure storage container create $container_name

echo "Uploading data"
azure storage blob upload nyc_taxi_data/trip_data_1.csv nyctaxihive trip_data_1.csv





