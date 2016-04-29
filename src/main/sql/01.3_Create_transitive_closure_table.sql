-- Uppdatera "endtime date" till "endtime date NOT NULL DEFAULT 'infinity'::date" och fixa följduppdateringarna.

-- Declare time variables.
DECLARE @startTime, @endTime, @startTimeEpoch;

-- Create and fill, relationships_isa, which is a modified table with only is a relationships.
SET @startTime = SELECT to_char(now(), 'YYYY-MM-DD HH24:MI:SS'), EXTRACT(EPOCH FROM now()); SET @startTimeEpoch = @startTime[0][1]; PRINT @startTime[0][0] + ' | Create and fill modified table with only is a relationships.';
--

CREATE TABLE relationships_isa (destinationid bigint NOT NULL, sourceid bigint NOT NULL, starttime date NOT NULL, endtime date NOT NULL DEFAULT 'infinity'::date, id bigint NOT NULL) WITH (OIDS=FALSE);
--
INSERT INTO relationships_isa (destinationid, sourceid, starttime, endtime, id) 
SELECT destinationid, sourceid, effectivetime AS starttime, 'infinity'::date AS endtime, id FROM relationships_rf2 WHERE typeid = 116680003 AND active = true;
--
UPDATE relationships_isa SET endtime = updateinfo.endtime 
FROM 
(
  SELECT start_table.id, start_table.effectivetime AS starttime, Min(end_table.effectivetime) AS endtime 
  FROM relationships_rf2 AS start_table JOIN relationships_rf2 AS end_table ON start_table.id = end_table.id AND start_table.effectivetime < end_table.effectivetime 
  GROUP BY start_table.id, starttime 
) AS updateinfo 
WHERE relationships_isa.id = updateinfo.id AND relationships_isa.starttime = updateinfo.starttime;
--
ALTER TABLE relationships_isa DROP COLUMN id;
--
CREATE INDEX index_relationships_isa_de ON relationships_isa USING btree (destinationid);
--
VACUUM FULL ANALYZE relationships_isa;

--
SET @endTime = SELECT to_char(now(), 'YYYY-MM-DD HH24:MI:SS'), EXTRACT(EPOCH FROM now()) - @startTimeEpoch; PRINT @endTime[0][0] + ' | Create and fill modified table with only is a relationships. Execution time: ' + CAST (@endTime[0][1] AS STRING) + ' s'; PRINT '';

-- Create and fill transitiveclosure with base data from the relationships_isa table.
SET @startTime = SELECT to_char(now(), 'YYYY-MM-DD HH24:MI:SS'), EXTRACT(EPOCH FROM now()); SET @startTimeEpoch = @startTime[0][1]; PRINT @startTime[0][0] + ' | Fill transitiveclosure with base data from the relationships_isa table.';
--
CREATE TABLE transitiveclosure (sourceid bigint NOT NULL, destinationid bigint NOT NULL, starttime date NOT NULL, endtime date NOT NULL DEFAULT 'infinity'::date, directrelation boolean NOT NULL, iteration smallint NOT NULL) WITH (OIDS=FALSE);
GRANT ALL ON TABLE transitiveclosure TO postgres;
GRANT ALL ON TABLE transitiveclosure TO exprepgroup;
CREATE INDEX index_transitiveclosure_sode ON transitiveclosure USING btree (sourceid, destinationid);
CREATE INDEX index_transitiveclosure_itsode ON transitiveclosure USING btree (iteration, sourceid , destinationid);
--
INSERT INTO transitiveclosure (sourceid, destinationid, starttime, endtime, directrelation, iteration)
  SELECT sourceid, destinationid, starttime, endtime, true AS directrelation, 0 AS iteration FROM relationships_isa WHERE endtime IS NOT NULL;
--
ANALYZE transitiveclosure;
--
SET @endTime = SELECT to_char(now(), 'YYYY-MM-DD HH24:MI:SS'), EXTRACT(EPOCH FROM now()) - @startTimeEpoch; PRINT @endTime[0][0] + ' | Fill transitiveclosure with base data from the relationships_isa table. Execution time: ' + CAST (@endTime[0][1] AS STRING) + ' s'; PRINT '';  PRINT '';

-- DECLARE iteration variables.
DECLARE @i, @exi;
--
SET @i = 0;

-- Iterate until BREAK.
WHILE 1
BEGIN
	-- Increase the iteration counter.
	SET @i = @i + 1;

	-- Create new rows in transitiveclosure.
	SET @startTime = SELECT to_char(now(), 'YYYY-MM-DD HH24:MI:SS'), EXTRACT(EPOCH FROM now()); SET @startTimeEpoch = @startTime[0][1]; PRINT @startTime[0][0] + ', ' + CAST (@i AS STRING) + ' | Create new rows in transitiveclosure.';
	--
	INSERT INTO transitiveclosure (sourceid, destinationid, starttime, endtime, directrelation, iteration)
	(
	  SELECT DISTINCT 
	    relationships_isa.sourceid AS sourceid, transitiveclosure.destinationid AS destinationid, 
	    GREATEST(transitiveclosure.starttime, relationships_isa.starttime) AS starttime, LEAST(transitiveclosure.endtime, relationships_isa.endtime) AS endtime,
	    false AS directrelation, @i AS iteration
	  FROM
	    transitiveclosure JOIN relationships_isa ON 
	    transitiveclosure.sourceid = relationships_isa.destinationid AND
	    (GREATEST(transitiveclosure.starttime, relationships_isa.starttime) < LEAST(transitiveclosure.endtime, relationships_isa.endtime))
	  WHERE 
	    transitiveclosure.iteration = @i - 1
	);
	--
	ANALYZE transitiveclosure;
	--
	SET @endTime = SELECT to_char(now(), 'YYYY-MM-DD HH24:MI:SS'), EXTRACT(EPOCH FROM now()) - @startTimeEpoch; PRINT @endTime[0][0] + ', ' + CAST (@i AS STRING) + ' | Create new rows in transitiveclosure. Execution time: ' + CAST (@endTime[0][1] AS STRING) + ' s'; PRINT '';

	-- Remove rows from transitiveclosure that spans a shorter time period than another row.
	SET @startTime = SELECT to_char(now(), 'YYYY-MM-DD HH24:MI:SS'), EXTRACT(EPOCH FROM now()); SET @startTimeEpoch = @startTime[0][1]; PRINT @startTime[0][0] + ', ' + CAST (@i AS STRING) + ' | Remove rows from transitiveclosure that spans a shorter time period than another row.';
	--
	DELETE FROM transitiveclosure AS redundant
	USING transitiveclosure AS master
	WHERE
	  redundant.sourceid = master.sourceid AND redundant.destinationid = master.destinationid AND 
	  ( (redundant.directrelation = false) OR  (redundant.directrelation = TRUE AND master.directrelation = true) ) AND
	  ( (redundant.starttime >= master.starttime AND redundant.endtime <  master.endtime) OR
	    (redundant.starttime >  master.starttime AND redundant.endtime <= master.endtime) OR
	    (redundant.starttime =  master.starttime AND redundant.endtime  = master.endtime AND redundant.iteration > master.iteration)
	  );
	--
	ANALYZE transitiveclosure;
	--
	SET @endTime = SELECT to_char(now(), 'YYYY-MM-DD HH24:MI:SS'), EXTRACT(EPOCH FROM now()) - @startTimeEpoch; PRINT @endTime[0][0] + ', ' + CAST (@i AS STRING) + ' | Remove rows from transitiveclosure that spans a shorter time period than another row. Execution time: ' + CAST (@endTime[0][1] AS STRING) + ' s'; PRINT '';

	-- Check if any insertations were done during the last iteration. If not BREAK the iteration loop.
	SET @exi = SELECT Count(*) = 0 FROM transitiveclosure WHERE iteration = @i;
	PRINT '';
	IF @exi[0][0] = 't'
	BEGIN
		PRINT '';
		BREAK;
	END
END

-- Iterate until BREAK.
WHILE 1
BEGIN
	-- Increase the iteration counter.
	SET @i = @i + 1;

	-- Merge time periods in transitiveclosure.
	SET @startTime = SELECT to_char(now(), 'YYYY-MM-DD HH24:MI:SS'), EXTRACT(EPOCH FROM now()); SET @startTimeEpoch = @startTime[0][1]; PRINT @startTime[0][0] + ', ' + CAST (@i AS STRING) + ' | Merge time periods in transitiveclosure.';
	--
	INSERT INTO transitiveclosure (sourceid, destinationid, starttime, endtime, directrelation, iteration)
	(
		SELECT smaller.sourceid AS sourceid, smaller.destinationid AS destinationid, smaller.starttime AS starttime, larger.endtime AS endtime, smaller.directrelation AS directrelation, @i AS iteration
		FROM transitiveclosure AS smaller JOIN transitiveclosure AS larger ON 
		smaller.sourceid = larger.sourceid AND smaller.destinationid = larger.destinationid AND smaller.directrelation = larger.directrelation 
		AND smaller.starttime < larger.starttime AND smaller.endtime >= larger.starttime AND smaller.endtime < larger.endtime	
	);
	--
	ANALYZE transitiveclosure;
	--
	SET @endTime = SELECT to_char(now(), 'YYYY-MM-DD HH24:MI:SS'), EXTRACT(EPOCH FROM now()) - @startTimeEpoch; PRINT @endTime[0][0] + ', ' + CAST (@i AS STRING) + ' | Merge time periods in transitiveclosure. Execution time: ' + CAST (@endTime[0][1] AS STRING) + ' s'; PRINT '';

	-- Remove rows from transitiveclosure that spans a shorter time period than another row.
	SET @startTime = SELECT to_char(now(), 'YYYY-MM-DD HH24:MI:SS'), EXTRACT(EPOCH FROM now()); SET @startTimeEpoch = @startTime[0][1]; PRINT @startTime[0][0] + ', ' + CAST (@i AS STRING) + ' | Remove rows from transitiveclosure that spans a shorter time period than another row.';
	--
	DELETE FROM transitiveclosure AS redundant
	USING transitiveclosure AS master
	WHERE
	  redundant.sourceid = master.sourceid AND redundant.destinationid = master.destinationid AND 
	  ( (redundant.directrelation = false) OR  (redundant.directrelation = TRUE AND master.directrelation = true) ) AND
	  ( (redundant.starttime >= master.starttime AND redundant.endtime <  master.endtime) OR
	    (redundant.starttime >  master.starttime AND redundant.endtime <= master.endtime) OR
	    (redundant.starttime =  master.starttime AND redundant.endtime  = master.endtime AND redundant.iteration > master.iteration)
	  );
	--
	ANALYZE transitiveclosure;
	--
	SET @endTime = SELECT to_char(now(), 'YYYY-MM-DD HH24:MI:SS'), EXTRACT(EPOCH FROM now()) - @startTimeEpoch; PRINT @endTime[0][0] + ', ' + CAST (@i AS STRING) + ' | Remove rows from transitiveclosure that spans a shorter time period than another row. Execution time: ' + CAST (@endTime[0][1] AS STRING) + ' s'; PRINT '';

	-- Check if any insertations were done during the last iteration. If not BREAK the iteration loop.
	SET @exi = SELECT Count(*) = 0 FROM transitiveclosure WHERE iteration = @i;
	PRINT '';
	IF @exi[0][0] = 't'
	BEGIN
		PRINT '';
		BREAK;
	END

END

-- Drop relationships_isa.
SET @startTime = SELECT to_char(now(), 'YYYY-MM-DD HH24:MI:SS'), EXTRACT(EPOCH FROM now()); SET @startTimeEpoch = @startTime[0][1]; PRINT @startTime[0][0] + ' | Drop relationships_isa.';
--
DROP TABLE relationships_isa;
--
SET @endTime = SELECT to_char(now(), 'YYYY-MM-DD HH24:MI:SS'), EXTRACT(EPOCH FROM now()) - @startTimeEpoch; PRINT @endTime[0][0] + ' | Drop relationships_isa. Execution time: ' + CAST (@endTime[0][1] AS STRING) + ' s'; PRINT '';

-- Update columns and index in transitiveclosure.
SET @startTime = SELECT to_char(now(), 'YYYY-MM-DD HH24:MI:SS'), EXTRACT(EPOCH FROM now()); SET @startTimeEpoch = @startTime[0][1]; PRINT @startTime[0][0] + ' | Update columns and index in transitiveclosure.';
--
DROP INDEX index_transitiveclosure_sode;
DROP INDEX index_transitiveclosure_itsode;
ALTER TABLE transitiveclosure DROP COLUMN iteration;
ALTER TABLE transitiveclosure ALTER COLUMN starttime TYPE timestamp without time zone;
ALTER TABLE transitiveclosure ALTER COLUMN endtime TYPE timestamp without time zone;
ALTER TABLE transitiveclosure ALTER COLUMN endtime SET DEFAULT 'infinity'::timestamp without time zone;
--
SET @endTime = SELECT to_char(now(), 'YYYY-MM-DD HH24:MI:SS'), EXTRACT(EPOCH FROM now()) - @startTimeEpoch; PRINT @endTime[0][0] + ' | Update columns and index in transitiveclosure. Execution time: ' + CAST (@endTime[0][1] AS STRING) + ' s'; PRINT '';

-- Create index index in transitiveclosure.
SET @startTime = SELECT to_char(now(), 'YYYY-MM-DD HH24:MI:SS'), EXTRACT(EPOCH FROM now()); SET @startTimeEpoch = @startTime[0][1]; PRINT @startTime[0][0] + ' | Create index index in transitiveclosure.';
--
CREATE INDEX index_transitiveclosure_sode ON transitiveclosure USING btree (sourceid, destinationid);
CREATE INDEX index_transitiveclosure_de ON transitiveclosure USING btree (destinationid);
CREATE INDEX index_transitiveclosure_starttime ON transitiveclosure USING btree (starttime DESC NULLS LAST);
CREATE INDEX index_transitiveclosure_endtime ON transitiveclosure USING btree (endtime DESC NULLS LAST);
--
SET @endTime = SELECT to_char(now(), 'YYYY-MM-DD HH24:MI:SS'), EXTRACT(EPOCH FROM now()) - @startTimeEpoch; PRINT @endTime[0][0] + ' | Create index index in transitiveclosure. Execution time: ' + CAST (@endTime[0][1] AS STRING) + ' s'; PRINT '';

-- Vacuum full analyze transitiveclosure.
SET @startTime = SELECT to_char(now(), 'YYYY-MM-DD HH24:MI:SS'), EXTRACT(EPOCH FROM now()); SET @startTimeEpoch = @startTime[0][1]; PRINT @startTime[0][0] + ' | Vacuum transitiveclosure.';
--
VACUUM FULL ANALYZE transitiveclosure;
--
SET @endTime = SELECT to_char(now(), 'YYYY-MM-DD HH24:MI:SS'), EXTRACT(EPOCH FROM now()) - @startTimeEpoch; PRINT @endTime[0][0] + ', ' + CAST (@i AS STRING) + ' | Vacuum transitiveclosure. Execution time: ' + CAST (@endTime[0][1] AS STRING) + ' s'; PRINT '';
