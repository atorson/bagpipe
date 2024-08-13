BEGIN;

CREATE TABLE IF NOT EXISTS audit(
      namekey VARCHAR(255) PRIMARY KEY NOT NULL,
      crt TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT clock_timestamp(),
      upd TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT clock_timestamp(),
      ver INTEGER NOT NULL DEFAULT 1
  );

CREATE TABLE IF NOT EXISTS statistic(
      namekey VARCHAR(255) PRIMARY KEY NOT NULL,
      audit VARCHAR(255) NOT NULL,
      family  VARCHAR(255) NOT NULL,
      time_from TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT clock_timestamp(),
      time_to TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT clock_timestamp(),
      current_value REAL NOT NULL,
      status VARCHAR(255) NOT NULL
  );

CREATE OR REPLACE FUNCTION row_upd()
RETURNS TRIGGER
AS
$$
BEGIN
    NEW.upd = clock_timestamp();
    RETURN NEW;
END;
$$
language 'plpgsql';

CREATE OR REPLACE FUNCTION row_ver()
RETURNS TRIGGER
AS
$$
BEGIN
    NEW.ver := NEW.ver +1;
    RETURN NEW;
END;
$$
language 'plpgsql';

CREATE TRIGGER row_upd_on_audit
BEFORE UPDATE
ON audit
FOR EACH ROW
EXECUTE PROCEDURE row_upd();

CREATE TRIGGER row_ver_on_audit
BEFORE UPDATE
ON audit
FOR EACH ROW
EXECUTE PROCEDURE row_ver();

COMMIT;

BEGIN;

CREATE TABLE IF NOT EXISTS trip(
      namekey VARCHAR(255) PRIMARY KEY NOT NULL,
      audit VARCHAR(255) NOT NULL,
      origin  VARCHAR(255) NOT NULL,
      destination VARCHAR(255) NOT NULL,
      current_state VARCHAR(255) NOT NULL,
      status  VARCHAR(255) NOT NULL
  );

CREATE TABLE IF NOT EXISTS car(
      namekey VARCHAR(255) PRIMARY KEY NOT NULL,
      audit VARCHAR(255) NOT NULL,
      last_known_state VARCHAR(255) NOT NULL,
      status  VARCHAR(255) NOT NULL
  );

CREATE TABLE IF NOT EXISTS location(
      namekey VARCHAR(255) PRIMARY KEY NOT NULL,
      x INTEGER NOT NULL,
      y INTEGER NOT NULL
  );

CREATE TABLE IF NOT EXISTS trip_state(
      namekey VARCHAR(255) PRIMARY KEY NOT NULL,
      audit VARCHAR(255) NOT NULL,
      trip  VARCHAR(255) NOT NULL,
      car VARCHAR(255) NOT NULL,
      trigger_action VARCHAR(255) NOT NULL,
      current_position VARCHAR(255) NOT NULL,
      status VARCHAR(255) NOT NULL
  );

CREATE TABLE IF NOT EXISTS trip_action(
      namekey VARCHAR(255) PRIMARY KEY NOT NULL,
      audit VARCHAR(255) NOT NULL,
      trip  VARCHAR(255) NOT NULL,
      car VARCHAR(255) NOT NULL,
      trigger_state VARCHAR(255) NOT NULL,
      status VARCHAR(255) NOT NULL
  );

CREATE TABLE IF NOT EXISTS trip_state_position(
      namekey VARCHAR(255) PRIMARY KEY NOT NULL,
      trip_state  VARCHAR(255) NOT NULL,
      car_location VARCHAR(255) NOT NULL
  );

COMMIT;
