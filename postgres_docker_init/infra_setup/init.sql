-- create schema staging
CREATE SCHEMA IF NOT EXISTS  staging;


-- create usa social security applications name data table
CREATE table IF NOT EXISTS staging.usa_names (
 state VARCHAR(2) NULL, -- VARCHAR(2) for fixed-length 2-digit state codes
  gender VARCHAR(1) NULL, -- VARCHAR(1) for fixed-length sex indicator
  year INTEGER NULL,
  name VARCHAR NULL,
  number INTEGER NULL

);

--insert data into table
COPY staging.usa_names (state, gender, year,name,number)
FROM '/data/usa_names.csv' DELIMITER ',' CSV HEADER;