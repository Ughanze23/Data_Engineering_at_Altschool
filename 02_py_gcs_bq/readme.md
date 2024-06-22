# Manage Google cloud storage (GCS) and Bigquery resource with python


## Description
Created 2 Python classes to manage Google cloud storage and Bigquery resource. 
A GCS class that lets you create,delete a bucket and upload files to a bucket.
A Bigquery class that lets you create , delete (datasets and tables) and upload data into biquery tables.
Finally a simple pipeline is designed to : create a bucket > load data into bucket > load data from bucket into biquery table.

## Table of Contents
- [Installation](#installation)
- [Usage](#usage)
- [Features](#features)
- [Reference](#reference)

1. **Clone the repository:**
    

2. **Navigate to the project directory:**
   

3. **Install the dependencies:**
    ```bash
    pip install -r requirements.txt
    ```

4. **Set up environment variables:**
    Create a `.env` file in the root directory and add the necessary environment variables. follow the .env.example file as a guide

5. **Set up config variables:**
    Update the config.py file with your own variables.

6. **Set up json Schemas:**
    Create your own Bigquery json schema files


## Usage

1. **Run the Query:**
    ```
    python main.py
    ```

## Features
* requirements.txt
* schemas folder: contains biquery Json schema files
* data folder: contains csv file uploaded  to bigquery
* bq_manager.py: bigquery resource manager python class
* gcs_manager.py : Google Cloud storage manager python class
* config.py : contains constants used in main.py 
* rest_api.py : REST API class
* get_request_api.py : Functions to make a get request to an API
* test.py : Simple data pipelineu test
* main.py : Simple data pipeline

## Reference
The data inserted into the the first table was obtained from google bigquery public data set.
The table contains the number of applicants for a Social Security card by year of birth and sex. The number of such applicants is restricted to U.S. births where the year of birth, sex, State of birth (50 States and District of Columbia) are known, and where the given name is at least 2 characters long.
source: http://www.ssa.gov/OACT/babynames/limits.html

The second data loaded into GCS and uploaded to Bigquery is from https://sampleapis.com/api-list/playstation.
Contains data of games released on playstation
