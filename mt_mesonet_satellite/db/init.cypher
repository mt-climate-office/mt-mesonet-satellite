match(n) detach delete n;
DROP INDEX timestampIndex;
DROP INDEX obsStationIndex;
DROP INDEX stationIndex;
DROP INDEX platformIndex;

CREATE INDEX timestampIndex FOR (o:Observation) on (o.timestamp);
CREATE INDEX obsStationIndex FOR (o:Observation) on (o.station);
CREATE INDEX stationIndex FOR (s:Station) ON (s.name);
CREATE INDEX platformIndex FOR (p:Platform) ON (p.name);

LOAD CSV WITH HEADERS FROM "file:///data_init.csv" AS line
MERGE (platform:Platform {name: line.platform})
MERGE (station:Station {name: line.station})
CREATE (obs:Observation {timestamp: toInteger(line.timestamp), element: line.element, value: toFloat(line.value), units: line.units})
CREATE (station)-[:HAS_PLATFORM]->(platform)-[:OBSERVES]->(obs)
