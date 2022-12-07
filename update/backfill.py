#!/usr/local/bin/python

import argparse
import datetime as dt
import os
import sys
from pathlib import Path
from typing import List, NoReturn

import numpy as np
import pandas as pd
from dotenv import load_dotenv
from loguru import logger
from mt_mesonet_satellite import MesonetSatelliteDB, Session, operational_update
from neo4j.exceptions import ConfigurationError

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


def backfill_isolated(stations: List[str], session: Session, conn: MesonetSatelliteDB):
    operational_update(conn=conn, session=session, backfill=True, stations=stations)


if __name__ == "__main__":

    parser = argparse.ArgumentParser("Backfilll new station data.")
    parser.add_argument(
        "-s",
        "--stations",
        nargs="+",
        help="The stations to backfill. If more than one station is being backfilled, separate names with a space.",
        required=True,
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
        station_df = pd.read_csv(
            "https://mesonet.climate.umt.edu/api/v2/stations?type=csv"
        )

        collocated = station_df.groupby(["latitude", "longitude"])
        collocated = collocated.agg(
            {
                "station": lambda x: np.unique(x).tolist(),
            }
        ).reset_index()

        collocated = collocated[collocated.station.str.len() >= 2]
        collocated_d = {}
        isolated_l = []

        for station in args.stations:
            tmp = collocated[collocated["station"].apply(lambda y: station in y)]
            if tmp.shape[0] == 1:
                collocated_d[station] = tmp.station.to_list()[0]
            else:
                isolated_l.append(station)

        if collocated_d:
            for station, pair in collocated_d.items():
                other = [x for x in pair if x != station][0]
                print(f"Collocated: {station}\nwith: {other}")
                backfill_collocated(station=station, collocated=other, conn=conn)

        if isolated_l:
            print(f"Isolated: {isolated_l}")
            backfill_isolated(stations=isolated_l, session=session, conn=conn)

    finally:
        session.logout()
        conn.close()
