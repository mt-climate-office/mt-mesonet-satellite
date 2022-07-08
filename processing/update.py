#!/usr/local/bin/python

import logging
import os

from dotenv import load_dotenv
from neo4j.exceptions import ConfigurationError

from mt_mesonet_satellite import MesonetSatelliteDB, Session, operational_update

load_dotenv("/setup/.env")

logger = logging.getLogger("mt_mesonet_satellite")

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
