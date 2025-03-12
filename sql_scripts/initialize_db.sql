/*
==============================================================
                            WARNING
==============================================================
Executing this script will drop database video_comments_wh
and re-create it from scratch. All data saved in the database
will be lost. 
*/
DROP DATABASE IF EXISTS video_comments_wh;
CREATE DATABASE video_comments_wh
    WITH 
    OWNER = postgres
    ENCODING = 'UTF8'
    TABLESPACE = pg_default
    CONNECTION LIMIT = -1;

\connect video_comments_wh

CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;