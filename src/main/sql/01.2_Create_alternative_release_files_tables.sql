DROP TABLE IF EXISTS concepts;
CREATE TABLE concepts
(
  id bigint NOT NULL,
  starttime date NOT NULL,
  endtime date NOT NULL DEFAULT 'infinity'::date,
  moduleid bigint NOT NULL,
  definitionstatusid bigint NOT NULL,
  CONSTRAINT "PK_concepts" PRIMARY KEY (id, starttime)
)
WITH (
  OIDS=FALSE
);
ALTER TABLE concepts OWNER TO postgres;
GRANT ALL ON TABLE concepts TO postgres;
GRANT ALL ON TABLE concepts TO exprepgroup;
--
INSERT INTO concepts (id, starttime, endtime, moduleid, definitionstatusid) 
  SELECT id, effectivetime AS starttime, 'infinity'::date AS endtime, moduleid, definitionstatusid FROM concepts_rf2 WHERE active = true;
--
UPDATE concepts SET endtime = updateinfo.endtime 
FROM 
(
  SELECT start_table.id, start_table.effectivetime AS starttime, Min(end_table.effectivetime) AS endtime 
  FROM concepts_rf2 AS start_table JOIN concepts_rf2 AS end_table ON start_table.id = end_table.id AND start_table.effectivetime < end_table.effectivetime 
  GROUP BY start_table.id, starttime 
) AS updateinfo 
WHERE concepts.id = updateinfo.id AND concepts.starttime = updateinfo.starttime;

VACUUM FULL ANALYZE concepts;

----------------------------------------
/*
DROP TABLE IF EXISTS relationships;
CREATE TABLE relationships
(
  id bigint NOT NULL,
  starttime date NOT NULL,
  endtime date NOT NULL DEFAULT 'infinity'::date,
  moduleid bigint NOT NULL,
  sourceid bigint NOT NULL,
  destinationid bigint NOT NULL,
  relationshipgroup integer NOT NULL,
  typeid bigint NOT NULL,
  characteristictypeid bigint NOT NULL,
  modifierid bigint NOT NULL,
  CONSTRAINT "PK_relationships" PRIMARY KEY (id, starttime)
)
WITH (
  OIDS=FALSE
);
ALTER TABLE relationships OWNER TO postgres;
GRANT ALL ON TABLE relationships TO postgres;
GRANT ALL ON TABLE relationships TO exprepgroup;
--
INSERT INTO relationships (id, starttime, endtime, moduleid, sourceid, destinationid, relationshipgroup, typeid, characteristictypeid, modifierid) 
  SELECT id, effectivetime AS starttime, 'infinity'::date AS endtime, moduleid, sourceid, destinationid, relationshipgroup, typeid, characteristictypeid, modifierid FROM relationships_rf2 WHERE active = true;
--
UPDATE relationships SET endtime = updateinfo.endtime 
FROM 
(
  SELECT start_table.id, start_table.effectivetime AS starttime, Min(end_table.effectivetime) AS endtime 
  FROM relationships_rf2 AS start_table JOIN relationships_rf2 AS end_table ON start_table.id = end_table.id AND start_table.effectivetime < end_table.effectivetime 
  GROUP BY start_table.id, starttime 
) AS updateinfo 
WHERE relationships.id = updateinfo.id AND relationships.starttime = updateinfo.starttime;

VACUUM FULL ANALYZE relationships;
*/