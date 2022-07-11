from pathlib import Path
from typing import Union

import pandas as pd
from neo4j import GraphDatabase
from neo4j.exceptions import ConstraintError


class MesonetSatelliteDB:
    def __init__(self, uri: str, user: str, password: str) -> None:
        """Initialize Mesonet Satellite DB object and connect to the Neo4j db.

        Args:
            uri (str): The database URI for the Neo4j database.
            user (str): The database Neo4j username.
            password (str): The database Neo4j password.
        """
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        """Close the connection to the Neo4j database."""
        self.driver.close()

    def init_db_indices(self):
        """Initialize index relationships and unique constraints."""
        with self.driver.session() as session:
            session.write_transaction(self._init_index)

    def init_db(self, f_dir: Union[str, Path], use_path: bool=False):
        """Initialize the Neo4j database using satellite data derived from the to_db_format.py script.

        Args:
            f_dir (Union[str, Path]): The directory with the 'data_init' files to save to the database.
        """
        with self.driver.session() as session:
            # Had to break file into multiple to keep from breaking.
            for f in Path(f_dir).glob("data_init*"):
                f_path = str(f) if use_path else f"file:///{f.name}"
                session.write_transaction(self._init_db, f_path)

    def query(
        self, station: str, start_time: int, end_time: int, element: str
    ) -> pd.DataFrame:
        """Query the Neo4j database for satellite observations at a station

        Args:
            station (str): The name of the Montana Mesonet station to query.
            start_time (int): The start time to begin the query formatted as seconds since 1970-01-01.
            end_time (int): The time to end the query formatted as seconds since 1970-01-01.
            element (str): The satellite indicator to gather data for.

        Returns:
            pd.DataFrame: A dataframe of the data returned from the query.
        """
        with self.driver.session() as session:

            try:
                response = session.write_transaction(
                    self._build_query,
                    station=station,
                    start_time=start_time,
                    end_time=end_time,
                    element=element,
                )
                dat = pd.DataFrame(response)
                dat.columns = [
                    "station",
                    "date",
                    "platform",
                    "element",
                    "value",
                    "units",
                ]
            except ValueError as e:
                print("No available data for this query.")
                dat = pd.DataFrame()
            return dat

    def post(self, dat: pd.DataFrame):
        """Write data to the Neo4j database.

        Args:
            dat (pd.DataFrame): Satellite data reformatted using the to_db_format function.
        """
        gen = dat.iterrows()
        for idx, row in gen:
            if idx % 10 == 0:
                print(f"{(idx/len(dat))*100:2.3f}% of New Observations Uploaded")
            try:
                with self.driver.session() as session:
                    session.write_transaction(self._post_data, **row.to_dict())
            except ConstraintError as e:
                print(e)

    def get_latest(self):
        with self.driver.session() as session:
            response = session.write_transaction(self._get_latest)
            dat = pd.DataFrame(response)

        dat.columns = ["date", "platform", "element"]
        dat = dat.assign(date=pd.to_datetime(dat.date, unit="s"))

        return dat

    @staticmethod
    def _get_latest(tx):
        result = tx.run(
            """
            MATCH (s:Station)-[o:OBSERVES]->(obs:Observation)\n
            RETURN MAX(o.timestamp) as time, obs.platform as platform, obs.element as element\n
            ORDER BY time
            """
        )

        return result.values()

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
            "RETURN s.name, o.timestamp, obs.platform,  obs.element, obs.value, obs.units",
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
