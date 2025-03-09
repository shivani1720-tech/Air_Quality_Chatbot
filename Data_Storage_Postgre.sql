CREATE DATABASE air_quality_db;

CREATE TABLE air_quality (
    Year INT,
    Month INT,
    Day INT,
    Hour TEXT,
    Station TEXT,
    O3_ppb FLOAT,
    PM2_5 FLOAT,
    NO2_ppb FLOAT,
    SO2_ppb FLOAT,
    CO_ppm FLOAT
);

#The below prompt execute in pg cmd 
COPY air_quality
FROM 'C:\Users\getsh\OneDrive\Desktop\PBL\Data\Air_Quality_Data\merged_air_quality_data_2012_2025.csv'
DELIMITER ','
CSV HEADER;
