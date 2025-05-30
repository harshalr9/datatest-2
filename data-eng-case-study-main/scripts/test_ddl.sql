
CREATE TABLE trips (
    vendor_id INTEGER,
    pickup_datetime TIMESTAMP,
    dropoff_datetime TIMESTAMP,
    passenger_count INTEGER,
    trip_distance FLOAT,
    pu_location_id INTEGER,
    do_location_id INTEGER,
    fare_amount FLOAT,
    tip_amount FLOAT,
    total_amount FLOAT
);

CREATE TABLE zones (
    location_id INTEGER PRIMARY KEY,
    borough TEXT,
    zone TEXT,
    service_zone TEXT
);
