/*
==============================================================
                            WARNING
==============================================================
Executing this script will drop database video_comments_wh
and re-create it from scratch. All data saved in the database
will be lost. 
*/
DO
$$BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_database WHERE lower(datname) = lower('video_comments_wh')) THEN
        CREATE DATABASE video_comments_wh
            WITH 
            OWNER = postgres
            ENCODING = 'UTF8'
            TABLESPACE = pg_default
            CONNECTION LIMIT = -1;
    END IF;
END$$;