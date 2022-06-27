from neo4j import GraphDatabase
import pandas as pd
from pathlib import Path
from neo4j.exceptions import ConstraintError


class MeonetSatelliteDB:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def init_db(self, f_dir):
        with self.driver.session() as session:
            session.write_transaction(self._init_index)
            # Had to break file into multiple to keep from breaking.
            for f in Path(f_dir).glob("data_init*"):
                f_path = f"file:///{f.name}"
                session.write_transaction(self._init_db, f_path)

    def query(self, **kwargs):
        with self.driver.session() as session:
            response = session.write_transaction(self._build_query, **kwargs)
            dat = pd.DataFrame(response)
            dat.columns = ["value", "date", "station", "platform", "element"]

            return dat

    def post(self, dat: pd.DataFrame):
        with driver.driver.session() as session:
            for idx, row in dat.iterrows():
                print(f"{(idx/len(dat))*100:2.3f}%")
                try:
                    session.write_transaction(driver._post_data, **row.to_dict())
                except ConstraintError as e:
                    print(e)
                    continue

    @staticmethod
    def _post_data(tx, **kwargs):
        tx.run(
            "MERGE (s:Station {name: $station}) "
            "MERGE (o:Observation {id: $id, platform: $platform, element: $element, value: $value, units: $units}) "
            "MERGE (s)-[:OBSERVES{timestamp: toInteger($timestamp)}]->(o);",
            **kwargs,
        )

    @staticmethod
    def _build_query(tx, **kwargs):
        result = tx.run(
            "MATCH p = (obs:Observation)<-[o:OBSERVES]-(s:Station) "
            "WHERE o.timestamp >= $start_time and o.timestamp <= $end_time and s.name = $station and obs.element = $element "
            "RETURN s.name, o.timestamp, obs.platform,  obs.element, obs.value",
            **kwargs,
        )
        return result.values()

    @staticmethod
    def _init_index(tx):
        tx.run("CREATE INDEX timestampIndex FOR (o:OBSERVES) on (o.timestamp); ")
        tx.run(
            "CREATE CONSTRAINT obsIdConstraint "
            "FOR (obs:Observation) "
            "REQUIRE obs.id IS UNIQUE; "
        )
        tx.run(
            "CREATE CONSTRAINT stationConstraint "
            "FOR (s:Station) "
            "REQUIRE s.name IS UNIQUE; "
        )

    @staticmethod
    def _init_db(tx, f_path):
        print(f_path)
        tx.run(
            "LOAD CSV WITH HEADERS FROM $f_path AS line "
            "MERGE (station:Station {name: line.station}) "
            "CREATE (obs:Observation {id: line.id, platform: line.platform, element: line.element, value: toFloat(line.value), units: toString(line.units)}) "
            "CREATE (station)-[:OBSERVES {timestamp: toInteger(line.timestamp)}]->(obs) ",
            f_path=f_path,
        )


driver = MeonetSatelliteDB("bolt://localhost:7687", "neo4j", "test")
# response = driver.query(start_time=1577862000, end_time=1580540400, station='aceingom', element='GPP')
driver.close()
