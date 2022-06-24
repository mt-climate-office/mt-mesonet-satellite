:auto match(n) 
CALL {
    WITH n
    DETACH DELETE n
} IN TRANSACTIONS OF 5000 ROWS
DROP INDEX timestampIndex;
DROP INDEX stationIndex;
DROP CONSTRAINT obsIdConstraint

CREATE INDEX timestampIndex FOR (obs:Observation) on (obs.timestamp);
CREATE INDEX stationIndex FOR (s:Station) ON (s.name);
CREATE CONSTRAINT obsIdConstraint
FOR (obs:Observation) 
REQUIRE obs.id IS UNIQUE;

:auto LOAD CSV WITH HEADERS FROM "file:///data_init.csv" AS line
CALL {
    WITH line
    MERGE (station:Station {name: line.station})
    CREATE (obs:Observation {id: line.id, platform: line.platform, element: line.element, value: toFloat(line.value), units: line.units})
    CREATE (station)-[:OBSERVES {timestamp: toInteger(line.timestamp)}]->(obs)
} IN TRANSACTIONS OF 250 ROWS