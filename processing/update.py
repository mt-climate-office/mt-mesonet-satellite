#!/usr/local/bin/python

from mt_mesonet_satellite import operational_update, MesonetSatelliteDB, Session
import logging
import os
from neo4j.exceptions import ConfigurationError


logging.basicConfig(
    level=logging.DEBUG,
    filename="/setup/log.txt",
    filemode="w",
    format="%(asctime)s %(message)s",
)

try:
    conn = MesonetSatelliteDB(
        uri=os.getenv("Neo4jURI"),
        user=os.getenv("Neo4jUser"),
        password=os.getenv("Neo4jPassword"),
    )
except ConfigurationError as e:
    logging.error(e)
    logging.error("Unable to connect to Neo4j DB.")

try:
    session = Session(dot_env=True)
except AssertionError as e:
    logging.error(e)

operational_update(conn=conn, session=session)

session.logout()
conn.close()
