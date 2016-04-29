DROP TABLE IF EXISTS concepts_rf2;
CREATE TABLE concepts_rf2
(
  id bigint NOT NULL,
  effectivetime date NOT NULL,
  active boolean NOT NULL,
  moduleid bigint NOT NULL,
  definitionstatusid bigint NOT NULL
)
WITH (
  OIDS=FALSE
);
ALTER TABLE concepts_rf2 OWNER TO postgres;
GRANT ALL ON TABLE concepts_rf2 TO postgres;
GRANT ALL ON TABLE concepts_rf2 TO exprepgroup;

DROP TABLE IF EXISTS relationships_rf2;
CREATE TABLE relationships_rf2
(
  id bigint NOT NULL,
  effectivetime date NOT NULL,
  active boolean NOT NULL,
  moduleid bigint NOT NULL,
  sourceid bigint NOT NULL,
  destinationid bigint NOT NULL,
  relationshipgroup integer NOT NULL,
  typeid bigint NOT NULL,
  characteristictypeid bigint NOT NULL,
  modifierid bigint NOT NULL
)
WITH (
  OIDS=FALSE
);
ALTER TABLE relationships_rf2 OWNER TO postgres;
GRANT ALL ON TABLE relationships_rf2 TO postgres;
GRANT ALL ON TABLE relationships_rf2 TO exprepgroup;