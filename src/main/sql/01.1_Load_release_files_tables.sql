COPY concepts_rf2 FROM 
:f1
WITH DELIMITER AS E'\t' CSV HEADER QUOTE AS '|';

COPY relationships_rf2 FROM
:f2
WITH DELIMITER AS E'\t' CSV HEADER QUOTE AS '`';

VACUUM FULL ANALYZE;

/*

COPY concepts_rf2 FROM 
E'C:\\Users\\Public\\Documents\\sct2_Concept_Full_INT_20160131.txt'
WITH DELIMITER AS E'\t' CSV HEADER QUOTE AS '|';

COPY relationships_rf2 FROM
E'C:\\Users\\Public\\Documents\\sct2_Relationship_Full_INT_20160131.txt'
WITH DELIMITER AS E'\t' CSV HEADER QUOTE AS '`';

VACUUM FULL ANALYZE;

 */
