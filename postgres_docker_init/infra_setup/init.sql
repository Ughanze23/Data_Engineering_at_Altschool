-- create schema staging
CREATE SCHEMA IF NOT EXISTS  staging;


-- create usa social security applications name data table
CREATE table IF NOT EXISTS usa_names (
 state CHAR(2) NULL, -- char(2) for fixed-length 2-digit state codes
  gender CHAR(1) NULL, -- char(1) for fixed-length sex indicator
  year INTEGER NULL,
  name VARCHAR NULL,
  number INTEGER NULL

)

--insert data into table
COPY staging.usa_names (state, gender, year,name,number)
FROM '/data/usa_names.csv' DELIMITER ',' CSV HEADER;