DROP TYPE IF EXISTS hardware_type CASCADE;
CREATE TYPE hardware_type AS ENUM ('TAPMV4.0', 'TAPV3.0', 'TAPMV3.0');
DROP TYPE IF EXISTS rf_test_type CASCADE;
CREATE TYPE rf_test_type AS ENUM ('RX', 'TX');

DROP TABLE IF EXISTS stations CASCADE;
CREATE TABLE stations (
    station_id  varchar(8) PRIMARY KEY,
    hardware    hardware_type NOT NULL,
    board_id    varchar(32) UNIQUE
);

DROP TABLE IF EXISTS ptus CASCADE;
CREATE TABLE ptus (
    ptu_id      SERIAL PRIMARY KEY,
    ptu_serial  INTEGER NOT NULL,
    ptu_sw_ver  varchar(32) NOT NULL,
    UNIQUE(ptu_serial, ptu_sw_ver)
);

DROP TABLE IF EXISTS utbs CASCADE;
CREATE TABLE utbs (
    utb_id      SERIAL PRIMARY KEY,
    hostname    varchar(32) NOT NULL,
    utb_sw_ver  varchar(8) NOT NULL,
    ems         varchar(32) NOT NULL,
    UNIQUE(hostname, utb_sw_ver)
);

DROP TABLE IF EXISTS runs CASCADE;
CREATE TABLE runs (
    run_id      serial PRIMARY KEY,
    station_id  varchar(8) NOT NULL REFERENCES stations ON DELETE CASCADE,
    ptu_id      integer NOT NULL REFERENCES ptus ON DELETE CASCADE,
    utb_id      integer NOT NULL REFERENCES utbs ON DELETE CASCADE,
    start       timestamp without time zone NOT NULL,
    duration    integer NOT NULL,
    outcome_val integer NOT NULL,
    outcome_msg varchar(256) NOT NULL,
    mongo_id    varchar(24) NOT NULL,
    year_week   varchar(7) NOT NULL,
    year_month  varchar(7) NOT NULL,
    order_id    varchar(32) NOT NULL
);

DROP TABLE IF EXISTS verifications CASCADE;
CREATE TABLE verifications (
    verification_id serial PRIMARY KEY,
    run_id          integer NOT NULL REFERENCES runs ON DELETE CASCADE,
    type            rf_test_type NOT NULL,
    name            varchar(32) NOT NULL,
    stop_if_fail    boolean NOT NULL,
    max_retries     smallint NOT NULL
);

DROP TABLE IF EXISTS verification_results CASCADE;
CREATE TABLE verification_results (
    verification_result_id serial PRIMARY KEY,
    verification_id        integer NOT NULL REFERENCES verifications ON DELETE CASCADE,
    fc                     integer NOT NULL,
    temperature            float NOT NULL,
    temperature_min        float NOT NULL,
    consumption            float NOT NULL,
    consumption_max        float NOT NULL,
    ok                     boolean NOT NULL
);

DROP TABLE IF EXISTS verification_measures CASCADE;
CREATE TABLE verification_measures (
    verification_result_id integer NOT NULL REFERENCES verification_results ON DELETE CASCADE,
    name                   varchar(32),
    foff                   integer NOT NULL,
    power                  integer NOT NULL,
    value                  float NOT NULL,
    min                    float,
    max                    float,
    ok                     boolean NOT NULL
);
