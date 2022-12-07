#!/usr/local/bin/python

import os
import sys
from pathlib import Path

from dotenv import load_dotenv
from loguru import logger
from mt_mesonet_satellite import MesonetSatelliteDB, Session, operational_update
from neo4j.exceptions import ConfigurationError

# from mt_mesonet_satellite import Task, Submit, clean_all, to_db_format, Product, Point


load_dotenv("/setup/.env")

f = Path("/setup/info.log")
f = str(f) if f.exists() else "./info.log"

logger.add(
    sys.stderr,
    format="{time:YYYY-MM-DD at HH:mm:ss} {level} {message}",
    level="INFO",
)
logger.add(
    f,
    format="{time:YYYY-MM-DD at HH:mm:ss} {level} {message}",
    level="INFO",
    rotation="100 MB",
)
logger.enable("mt_mesonet_satellite")

if __name__ == "__main__":

    try:
        conn = MesonetSatelliteDB(
            uri=os.getenv("Neo4jURI"),
            user=os.getenv("Neo4jUser"),
            password=os.getenv("Neo4jPassword"),
        )
    except ConfigurationError as e:
        logger.exception(e)
        logger.exception("Unable to connect to Neo4j DB.")

    try:
        session = Session(dot_env=True)
    except AssertionError as e:
        logger.exception(e)

    try:
        operational_update(conn=conn, session=session)
    finally:
        session.logout()
        conn.close()
