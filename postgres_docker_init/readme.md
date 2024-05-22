#Postgres setup with docker and psycopg2


## Description
This project is a simple infrastructure setup of a postgres database using docker. a simple database is created with a single table, and data is loaded into the table. the project also focuses on how to use python(psycopg2) to connect to the database , run a simple query to return the number of rows in the created table showing that the setup was a success.


## Table of Contents
- [Installation](#installation)
- [Usage](#usage)
- [Features](#features)
- [Reference](#reference)

## Installation

1. **Clone the repository:**
    

2. **Navigate to the project directory:**
   

3. **Install the dependencies:**
    ```bash
    pip install -r requirements.txt
    ```

4. **Set up environment variables:**
    Create a `.env` file in the root directory and add the necessary environment variables as per the `.env` file.

5. **Run the docker file:**
   run "docker-compose up" to startup the postgres database

## Usage

1. **Run the Query:**
    ```
    python main.py
    ```
## Features
requirements.txt
docker-compose.yml setup postgres database
init.sql : setup script to create schema, table and insert records
db_setup.py : connect to postgres db
main.py : query postgres db

## Reference
the table inserted into the database was obtained from google bigquery public data set.
The table contains the number of applicants for a Social Security card by year of birth and sex. The number of such applicants is restricted to U.S. births where the year of birth, sex, State of birth (50 States and District of Columbia) are known, and where the given name is at least 2 characters long.

source: http://www.ssa.gov/OACT/babynames/limits.html