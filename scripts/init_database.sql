/*
=============================================================
Create Databases for Medallion Architecture
=============================================================
Script Purpose:
    This script creates the databases required to implement
    the Medallion Architecture in MySQL.

    The architecture consists of three layers:
        - bronze : raw ingested data
        - silver : cleaned and standardized data
        - gold   : analytical and business-ready data

WARNING:
    Running this script will DROP the databases if they exist.
    All data will be permanently deleted.
    Make sure you have backups before executing this script.
=============================================================
*/

-- Drop existing databases (if they exist)

DROP DATABASE IF EXISTS bronze;
DROP DATABASE IF EXISTS silver;
DROP DATABASE IF EXISTS gold;

-- Create databases

CREATE DATABASE bronze;
CREATE DATABASE silver;
CREATE DATABASE gold;

-- Show created databases

SHOW DATABASES;

