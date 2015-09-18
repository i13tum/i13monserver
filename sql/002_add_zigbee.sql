

CREATE TABLE zigbeeplugs
(
  id uuid NOT NULL,
  macaddress VARCHAR(24) NOT NULL,
  ts time without time zone,
  load numeric(8,3),
  irms numeric(8,3),
  vrms numeric(8,3),
  freq numeric(8,5),
  pow VARCHAR(8),
  work numeric(8,3),
  CONSTRAINT pkzigbeeplugs_id PRIMARY KEY (id)
)
WITH (
  OIDS=FALSE
);
ALTER TABLE zigbeeplugs
  OWNER TO i13mon;

-- Index: asc_ts

-- DROP INDEX asc_ts;

CREATE INDEX zigbeeplugs_asc_ts
  ON zigbeeplugs
  USING btree
  (ts);

