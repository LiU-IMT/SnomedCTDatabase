CREATE SEQUENCE expressions_id_seq INCREMENT BY -1 MAXVALUE -1 MINVALUE -9223372036854775808 START WITH -1;
GRANT ALL ON SEQUENCE expressions_id_seq TO postgres;
GRANT ALL ON SEQUENCE expressions_id_seq TO exprepgroup;
CREATE TABLE expressions
(
  id bigint NOT NULL DEFAULT nextval('expressions_id_seq'),
  starttime timestamp without time zone NOT NULL,
  endtime timestamp without time zone NOT NULL DEFAULT 'infinity',
  expression text NOT NULL, 
  CONSTRAINT "PK_expressions" PRIMARY KEY (id, starttime)
)
WITH (
  OIDS=FALSE
);
ALTER SEQUENCE expressions_id_seq OWNED BY expressions.id;
ALTER TABLE expressions OWNER TO postgres;
GRANT ALL ON TABLE expressions TO postgres;
GRANT ALL ON TABLE expressions TO exprepgroup;

----------------------------------------

CREATE TABLE equivalents
(
  id bigint NOT NULL,
  starttime timestamp without time zone  NOT NULL,
  endtime timestamp without time zone NOT NULL DEFAULT 'infinity',
  equivalentid serial NOT NULL,
  CONSTRAINT "PK_equivalents" PRIMARY KEY (id, starttime)
)
WITH (
  OIDS=FALSE
);
ALTER TABLE equivalents OWNER TO postgres;
GRANT ALL ON TABLE equivalents TO postgres;
GRANT ALL ON TABLE equivalents TO exprepgroup;
GRANT ALL ON SEQUENCE equivalents_equivalentid_seq TO postgres;
GRANT ALL ON SEQUENCE equivalents_equivalentid_seq TO exprepgroup;
CREATE INDEX index_equivalents_equivalentid ON equivalents USING btree (equivalentid);

----------------------------------------

CREATE OR REPLACE VIEW eqv AS 
 SELECT equivalents1.id AS id1, equivalents2.id AS id2, GREATEST(equivalents1.starttime, equivalents2.starttime) AS starttime, LEAST(equivalents1.endtime, equivalents2.endtime) AS endtime
   FROM equivalents equivalents1 JOIN equivalents equivalents2 ON equivalents1.equivalentid = equivalents2.equivalentid AND equivalents1.id <> equivalents2.id
  WHERE GREATEST(equivalents1.starttime, equivalents2.starttime) < LEAST(equivalents1.endtime, equivalents2.endtime);
ALTER TABLE eqv OWNER TO postgres;
GRANT ALL ON TABLE eqv TO postgres;
GRANT ALL ON TABLE eqv TO exprepgroup;

----------------------------------------

CREATE OR REPLACE VIEW coneqv AS 
 SELECT concepts.id AS id1, concepts.id AS id2, concepts.starttime::timestamp without time zone AS starttime, concepts.endtime::timestamp without time zone AS endtime
   FROM concepts
UNION
 SELECT expressions.id AS id1, expressions.id AS id2, expressions.starttime, expressions.endtime
   FROM expressions
UNION
 SELECT eqv.id1, eqv.id2, eqv.starttime, eqv.endtime
   FROM eqv;
ALTER TABLE coneqv OWNER TO postgres;
GRANT ALL ON TABLE coneqv TO postgres;
GRANT ALL ON TABLE coneqv TO exprepgroup;