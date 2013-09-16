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
GRANT ALL ON TABLE concepts_rf2 TO termbindgroup;

DROP TABLE IF EXISTS descriptions_rf2;
CREATE TABLE descriptions_rf2
(
  id bigint NOT NULL,
  effectivetime date NOT NULL,
  active boolean NOT NULL,
  moduleid bigint NOT NULL,
  conceptid bigint NOT NULL,
  languagecode character(2) NOT NULL,
  typeid bigint NOT NULL,
  term text NOT NULL,
  caseSignificanceId bigint NOT NULL
)
WITH (
  OIDS=FALSE
);
ALTER TABLE descriptions_rf2 OWNER TO postgres;
GRANT ALL ON TABLE descriptions_rf2 TO postgres;
GRANT ALL ON TABLE descriptions_rf2 TO termbindgroup;

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
GRANT ALL ON TABLE relationships_rf2 TO termbindgroup;

DROP TABLE IF EXISTS languagerefsets_rf2;
CREATE TABLE languagerefsets_rf2
(
  id uuid NOT NULL,
  effectivetime date NOT NULL,
  active boolean NOT NULL,
  moduleid bigint NOT NULL,
  refsetid bigint NOT NULL,
  referencedcomponentid bigint NOT NULL,
  acceptabilityid bigint NOT NULL
)
WITH (
  OIDS=FALSE
);
ALTER TABLE languagerefsets_rf2 OWNER TO postgres;
GRANT ALL ON TABLE languagerefsets_rf2 TO postgres;
GRANT ALL ON TABLE languagerefsets_rf2 TO termbindgroup;

