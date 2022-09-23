#!/usr/local/bin/python

import os
import argparse
from pathlib import Path

from dotenv import load_dotenv
from loguru import logger
from neo4j.exceptions import ConfigurationError
import sys
from typing import NoReturn
import pandas as pd
import datetime as dt

from mt_mesonet_satellite import MesonetSatelliteDB, Session, operational_update, Point

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

ELEMENTS = [
    "NDVI",
    "EVI",
    "PET",
    "ET",
    "GPP",
    "LAI",
    "Fpar",
    "sm_surface_wetness",
    "sm_surface",
    "sm_rootzone_wetness",
    "sm_rootzone",
]


def backfill_collocated(
    station: str, collocated: str, conn: MesonetSatelliteDB
) -> NoReturn:
    """Backfill a collocated station with satellite data from the collocated station.

    Args:
        station (str): Name of the station to backfill.
        collocated (str): Name of the existing station with data in the database.
        conn (MesonetSatelliteDB): Connection to the Neo4j database.

    Returns:
        NoReturn: Nothing is returned, data are written to the database.
    """
    dfs = []
    now = round((dt.datetime.now() - dt.datetime(1970, 1, 1)).total_seconds())

    # Query all of the elements at a station.
    for element in ELEMENTS:
        print(element)
        tmp = conn.query(collocated, 0, now, element)
        dfs.append(tmp)

    dat = pd.concat(dfs, axis=0)

    # Reformat results to match the format of the to_db_format function.
    dat = dat.rename(columns={"date": "timestamp"})
    dat = dat[["station", "timestamp", "element", "value", "platform", "units"]]
    dat = dat.assign(units=dat.units.replace(r"^\s*$", "unitless", regex=True))
    dat = dat.assign(station=station)
    dat = dat.assign(
        id=dat.station
        + "_"
        + dat.timestamp.astype(str)
        + "_"
        + dat.platform
        + "_"
        + dat.element
    )
    dat = dat.reset_index(drop=True)

    # Post data to database.
    conn.post(dat)


def backfill_isolated(station):
    raise NotImplementedError("This method hasn't been implemented yet :(.")


def backfill_station(station: str, conn: MesonetSatelliteDB) -> NoReturn:
    """Determine if a station is collocated and backfill accordingly.

    Args:
        station (str): The name of the station to backfill.
        conn (MesonetSatelliteDB): Connection to Neo4j database.

    Returns:
        NoReturn: Nothing returned. Data are backfilled into the database.
    """
    # Get list of stations from the API.
    stations = pd.read_csv("https://mesonet.climate.umt.edu/api/v2/stations?type=csv")

    # Determine if the station is collocated.
    station_loc = stations[stations["station"] == station]
    station_loc = stations[
        (stations["longitude"] == station_loc["longitude"].values[0])
        & (stations["latitude"] == station_loc["latitude"].values[0])
    ]
    collocated_station = station_loc[station_loc["station"] != station][
        "station"
    ].values[0]

    # Backfill accordingly.
    if station_loc.shape[0] == 2:
        backfill_collocated(station, collocated_station, conn)
    else:
        backfill_isolated(station)


if __name__ == "__main__":

    parser = argparse.ArgumentParser("Backfilll new station data.")
    parser.add_argument(
        '-s','--stations', nargs='+',
         help='The stations to backfill. If more than one station is being backfilled, separate names with a space.',
        required=True
    )
    args = parser.parse_args()
    load_dotenv("./.env")
    
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
        operational_update(conn=conn, session=session, backfill=True, stations=args.stations)
    finally:
        session.logout()
        conn.close()

# load_dotenv("../setup/.env")

# conn = MesonetSatelliteDB(
#     uri=os.getenv("Neo4jURI"),
#     user=os.getenv("Neo4jUser"),
#     password=os.getenv("Neo4jPassword"),
# )

# backfill_station("aceoilmo", conn)
# backfill_station("acecoffe", conn)
# backfill_station("acetosto", conn)

# conn.close()