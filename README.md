SnomedCTDatabase
================

Scripts for installing the SNOMED CT Expression Repository database. Run 'mvn compile' to execute the database scripts. It it doesn't work in Eclipse, try the command line.

### Requirements:
1. A running PostgreSQL server.
2. Two propoperties files:

  * pwd.properties: includes user (and password) with permission to create databases etc.

  * snomedct.properties: includes path to SNOMED CT distribution files

Examples are found in the src/main/resources folder.
