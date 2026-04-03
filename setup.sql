-- Create a new database for the project
CREATE OR REPLACE DATABASE healthcare_db;

-- Use the created database
USE DATABASE healthcare_db;

-- Create a schema inside the database
CREATE OR REPLACE SCHEMA public_schema;

-- Use the schema
USE SCHEMA public_schema;

-- Create main table to store appointment data
CREATE OR REPLACE TABLE appointments(
    patient_id INT,              
    patient_name STRING,         
    appointment_date DATE,       
    appointment_time TIME,       
    status STRING                
);

-- Create an internal stage to upload files
CREATE OR REPLACE STAGE patient_stage;

-- Check if any files exist in the stage
LIST @patient_stage;

-- Create file format for reading CSV files
CREATE OR REPLACE FILE FORMAT patient_csv_format
TYPE = 'CSV'                         
FIELD_OPTIONALLY_ENCLOSED_BY = '"'   
SKIP_HEADER = 1;                     

-- Load data from stage into appointments table
COPY INTO appointments
FROM @patient_stage
FILE_FORMAT = patient_csv_format;

-- Create table to store attended patients
CREATE OR REPLACE TABLE attended_patients(
    patient_id INT,
    patient_name STRING,
    appointment_date DATE,
    appointment_time TIME,
    status STRING
);

-- Create table to store missed patients
CREATE OR REPLACE TABLE missed_patients (
    patient_id INT,
    patient_name STRING,
    appointment_date DATE,
    appointment_time TIME,
    status STRING
);
