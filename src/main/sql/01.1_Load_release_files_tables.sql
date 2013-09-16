COPY concepts_rf2 FROM
								E'${snomedCTSourceDirectory}Full/Terminology/sct2_Concept_Full_INT_20130731.txt'
								WITH
								DELIMITER AS E'\t' CSV HEADER QUOTE AS '|';

								COPY descriptions_rf2 FROM
								E'${snomedCTSourceDirectory}Full/Terminology/sct2_Description_Full-en_INT_20130731.txt'
								WITH DELIMITER AS E'\t' CSV HEADER QUOTE AS '`';

								COPY descriptions_rf2 FROM
								E'${snomedCTSourceDirectory}Full/Terminology/sct2_TextDefinition_Full-en_INT_20130731.txt'
								WITH DELIMITER AS E'\t' CSV HEADER QUOTE AS '`';

								COPY relationships_rf2 FROM
								E'${snomedCTSourceDirectory}Full/Terminology/sct2_Relationship_Full_INT_20130731.txt'
								WITH DELIMITER AS E'\t' CSV HEADER QUOTE AS '`';

								COPY languagerefsets_rf2 FROM
								E'${snomedCTSourceDirectory}Full/Refset/Language/der2_cRefset_LanguageFull-en_INT_20130731.txt'
								WITH DELIMITER AS E'\t' CSV HEADER QUOTE AS '`';
