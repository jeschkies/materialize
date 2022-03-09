CREATE SOURCE gateway_raw
FROM LOKI
ADDRESS 'https://logs-dev-005.grafana.net'
QUERY '{name="cortex-gw", namespace=~"cortex-dev.*"} |= "eventType=bi" | logfmt'
USER '29'
PASSWORD '...'
FORMAT TEXT;

CREATE SOURCE gateway_raw
FROM LOKI
ADDRESS 'https://logs-dev-005.grafana.net'
QUERY '{name="cortex-gw", namespace=~"cortex-dev.*"} |= "eventType=bi" | logfmt'
FORMAT TEXT;

CREATE VIEW gateway_bi_events AS
  SELECT
    val->>'line' AS text,
    val->'labels' AS labels
  FROM (SELECT text::jsonb AS val FROM gateway_raw);

CREATE MATERIALIZED VIEW bi_events AS
  SELECT CAST(labels->>'instanceID' AS int) AS instanceID,
    labels->>'writeCount' AS writes 
    FROM gateway_bi_events;

SELECT * FROM bi_events;

CREATE SOURCE gcom
FROM POSTGRES
  CONNECTION 'host=localhost port=5432 user=postgres password=mysecretpassword dbname=gcom'
  PUBLICATION 'instances_pub';

CREATE VIEWS FROM SOURCE gcom;
CREATE MATERIALIZED VIEW gcom_instances AS
  SELECT id AS instanceID, org_id, name
  FROM instances;


SELECT *
  FROM bi_events
  INNER JOIN gcom_instances
  ON bi_events.instanceID = gcom_instances.instanceID;
