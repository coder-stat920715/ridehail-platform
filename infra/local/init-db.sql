CREATE ROLE trip     LOGIN PASSWORD 'trip';
CREATE ROLE location LOGIN PASSWORD 'location';
CREATE ROLE payment  LOGIN PASSWORD 'payment';
CREATE ROLE matching LOGIN PASSWORD 'matching';
CREATE ROLE notification LOGIN PASSWORD 'notification';
CREATE DATABASE tripdb     OWNER trip;
CREATE DATABASE locationdb OWNER location;
CREATE DATABASE paymentdb  OWNER payment;
CREATE DATABASE matchingdb OWNER matching;
CREATE DATABASE notificationdb OWNER notification;
-- Local only: superuser so Debezium may create its publication. In prod use REPLICATION + a pre-created publication.
CREATE ROLE debezium LOGIN REPLICATION SUPERUSER PASSWORD 'debezium';
