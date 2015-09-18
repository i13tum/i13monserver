-- Table: rfmpi

-- DROP TABLE rfmpi;

CREATE TABLE rfmpi
(
  id uuid NOT NULL,
  deviceid uuid NOT NULL,
  ts time without time zone,
  power1 numeric(8,3),
  power2 numeric(8,3),
  power3 numeric(8,3),
  power4 numeric(8,3),
  vrms numeric(8,3),
  temp numeric(8,3),
  CONSTRAINT rfmpi_pk_id PRIMARY KEY (id)
)
WITH (
  OIDS=FALSE
);
ALTER TABLE rfmpi
  OWNER TO i13mon;

-- Index: asc_ts

-- DROP INDEX asc_ts;

CREATE INDEX rfmpi_asc_ts
  ON rfmpi
  USING btree
  (ts);

