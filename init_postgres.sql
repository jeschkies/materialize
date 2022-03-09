# docker run --rm --name postgres -e POSTGRES_PASSWORD=mysecretpassword -p 5432:5432 postgres -c wal_level=logical
CREATE DATABASE gcom;

CREATE TABLE instances (
  id integer NOT NULL PRIMARY KEY,
  org_id integer NOT NULL,
  name varchar(255) DEFAULT NULL
);

CREATE PUBLICATION instances_pub FOR TABLE instances;

INSERT INTO instances (id, org_id, name) VALUES
    (9960, 12574, 'cortex'),
    (295486, 12574, 'tempo');

