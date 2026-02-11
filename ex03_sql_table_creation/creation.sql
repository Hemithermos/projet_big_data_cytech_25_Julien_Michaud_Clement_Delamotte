BEGIN;

-- Star Schema

CREATE SCHEMA IF NOT EXISTS dw;
SET search_path = dw;

-- Nettoyer
DROP TABLE IF EXISTS fact_trip CASCADE;
DROP TABLE IF EXISTS dim_time CASCADE;
DROP TABLE IF EXISTS dim_date CASCADE;
DROP TABLE IF EXISTS dim_zone CASCADE;
DROP TABLE IF EXISTS dim_store_and_fwd CASCADE;
DROP TABLE IF EXISTS dim_payment_type CASCADE;
DROP TABLE IF EXISTS dim_ratecode CASCADE;
DROP TABLE IF EXISTS dim_vendor CASCADE;


-- dimensions

CREATE TABLE dim_vendor (
    vendor_id SMALLINT PRIMARY KEY,   -- from 0 to 7
    vendor_name TEXT NOT NULL
);

CREATE TABLE dim_ratecode (
    ratecode_id SMALLINT PRIMARY KEY, -- from 1 to 6 and 99
    ratecode_label TEXT NOT NULL
)

CREATE TABLE dim_payment_type (
    payment_type_id SMALLINT PRIMARY KEY, -- from 0 to 6
    payment_type_label
)

CREATE TABLE dim_store_and_fwd (
    store_and_fwd_flag CHAR(1) PRIMARY KEY, -- is 'Y' or 'N'
    label TEXT NOT NULL,
    CONSTRAINT ck_store_and_fwd_flag CHECK (store_and_fwd_flag IN ('Y', 'N'))
);

CREATE TABLE dim_zone (
    location_id INT PRIMARY KEY, -- from 1 to 266
    borough TEXT,
    zone_name TEXT,
    service_zone TEXT -- all three in the look up table
)

CREATE TABLE dim_date (
    date_key INT PRIMARY KEY, -- in YYYYMMDD format
    full_date DATE NOT NULL UNIQUE,
    year SMALLINT NOT NULL,
    month SMALLINT NOT NULL CHECK(month BETWEEN 1 AND 12),
    day SMALLINT NOT NULL CHECK (day BETWEEN 1 AND 31),
    day_of_week SMALLINT NOT NULL CHECK (day_of_week BETWEEN 1 AND 7), -- si on veut faire des analytics jour par jour genmre lundi vs dimanche
    is_weekend BOOLEAN NOT NULL -- pk pas
)

CREATE TABLE dim_time (
    time_key INT PRIMARY KEY, -- in HHMMSS format
    full_time TIME NOT NULL UNIQUE,
    hour SMALLINT NOT NULL CHECK (hour BETWEEN 0 AND 23),
    minute SMALLINT NOT NULL CHECK (minute BETWEEN 1 AND 59),
    second SMALLINT NOT NULL CHECK (second BETWEEN 1 AND 59)
)



-- table des faits

CREATE TABLE fact_trip ( -- big one
    trip_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,

    -- cles vers dimensions
    vendor_id SMALLINT NOT NULL REFERENCES dim_vendor(vendor_id),
    ratecode_id SMALLINT NOT NULL REFERENCES dim_ratecode(ratecode_id),
    payment_type_id SMALLINT NOT NULL REFERENCES dim_payment_type(payment_type_id),
    store_and_fwd_flag CHAR(1) NOT NULL REFERENCES dim_store_and_fwd(store_and_fwd_flag),

    pu_location_id INT NOT NULL REFERENCES dim_zone(location_id),
    do_location_id INT NOT NULL REFERENCES dim_zone(location_id),

    pu_date_key INT NOT NULL REFERENCES dim_date(date_key),
    do_date_key INT NOT NULL REFERENCES dim_date(date_key),
    pu_time_key INT NOT NULL REFERENCES dim_time(time_key),
    do_time_key INT NOT NULL REFERENCES dim_time(time_key),

    -- bien a garder pour controler 2 3 trucs
    pickup_datetime TIMESTAMP NOT NULL,
    dropoff_datetime TIMESTAMP NOT NULL,

    -- Mesures
    -- added sanity tests
    passenger_count SMALLINT NOT NULL DEFAULT 0 CHECK (passenger_count >= 0),
    trip_distance NUMERIC(10,3) NOT NULL DEFAULT 0 CHECK (trip_distance >= 0),
    fare_amount NUMERIC(10,2) NOT NULL DEFAULT 0 CHECK (fare_amount >= 0),
    extra NUMERIC(10,2) NOT NULL DEFAULT 0 CHECK (extra >= 0),
    mta_tax NUMERIC(10,2) NOT NULL DEFAULT 0 CHECK (mta_tax >= 0),
    tip_amount NUMERIC(10,2) NOT NULL DEFAULT 0 CHECK (tip_amount >= 0),
    tolls_amount NUMERIC(10,2) NOT NULL DEFAULT 0 CHECK (tolls_amount >= 0),
    improvement_surcharge NUMERIC(10,2) NOT NULL DEFAULT 0 CHECK (improvement_surcharge >= 0),
    congestion_surcharge NUMERIC(10,2) NOT NULL DEFAULT 0 CHECK (congestion_surcharge >= 0),
    airport_fee NUMERIC(10,2) NOT NULL DEFAULT 0 CHECK (airport_fee >= 0),
    cbd_congestion_fee NUMERIC(10,2) NOT NULL DEFAULT 0 CHECK (cbd_congestion_fee >= 0),

    total_amount NUMERIC(10,2) NOT NULL DEFAULT 0 CHECK (total_amount >= 0)
    
    -- big constraint, interdiction du voyage dans le temps
    CONSTRAINT ck_dropoff_after_pickup_test CHECK (dropoff_datetime >= pickup_datetime)

);



COMMIT;