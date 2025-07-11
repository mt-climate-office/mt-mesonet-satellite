#!/usr/local/bin/python

import argparse
import datetime as dt
import os
import sys
from pathlib import Path
from typing import List
import json

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
]

def convert_date_to_seconds(d: dt.date | dt.datetime | str) -> int:
    if isinstance(d, str):
        d = dt.datetime.strptime(d, "%Y-%m-%d")
    if isinstance(d, dt.date):
        d = dt.datetime.combine(d, dt.time.min)
    
    return int((d - dt.datetime(1970, 1, 1)).total_seconds())


def get_earliest_record(conn: MesonetSatelliteDB):
    stations = pd.read_csv(
        "https://mesonet.climate.umt.edu/api/v2/stations?type=csv"
    )

    date_records = {}
    for station in stations['station'].to_list():
        print(f"Processing {station}...")
        try:
            _ = conn.query(station, convert_date_to_seconds("2020-01-01"),
                    convert_date_to_seconds("2020-03-01"), "NDVI")
            d = dt.date(2000, 1, 1)
        except ValueError:
            d = dt.date.today()

        date_records[station] = d

def backfill_collocated(
    station: str, collocated: str, conn: MesonetSatelliteDB
) -> None:
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


def execute_backfill(stations: List[str], session: Session, conn: MesonetSatelliteDB):
    """Execute the backfill logic for a list of stations.
    
    Args:
        stations (List[str]): List of station names to backfill.
        session (Session): Session object for API access.
        conn (MesonetSatelliteDB): Connection to the Neo4j database.
    """
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

    for station in stations:
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


def check_and_backfill(session: Session, conn: MesonetSatelliteDB):
    """Check station record dates and backfill stations that need it.
    
    Args:
        session (Session): Session object for API access.
        conn (MesonetSatelliteDB): Connection to the Neo4j database.
    """
    # Load station record dates
    record_dates_path = Path("/setup/update/station_record_dates.json")
    if record_dates_path.exists():
        with open(record_dates_path, 'r') as f:
            station_record_dates = json.load(f)
    else:
        logger.warning(f"Station record dates file not found at {record_dates_path}")
        station_record_dates = {}
    
    # Get current stations from API
    station_df = pd.read_csv(
        "https://mesonet.climate.umt.edu/api/v2/stations?type=csv"
    )
    
    stations_to_backfill = []
    cutoff_date = dt.date(2020, 1, 1)
    
    for station in station_df['station'].to_list():
        needs_backfill = False
        
        if station not in station_record_dates:
            logger.info(f"Station {station} not found in record dates, adding to backfill list")
            needs_backfill = True
        else:
            # Parse the date from the record
            try:
                station_date = dt.datetime.strptime(station_record_dates[station], "%Y-%m-%d").date()
                if station_date > cutoff_date:
                    logger.info(f"Station {station} date {station_date} > {cutoff_date}, adding to backfill list")
                    needs_backfill = True
            except (ValueError, TypeError) as e:
                logger.warning(f"Could not parse date for station {station}: {e}, adding to backfill list")
                needs_backfill = True
        
        if needs_backfill:
            stations_to_backfill.append(station)
    
    if stations_to_backfill:
        logger.info(f"Found {len(stations_to_backfill)} stations needing backfill: {stations_to_backfill}")
        execute_backfill(stations_to_backfill, session, conn)
    else:
        logger.info("No stations need backfilling")


if __name__ == "__main__":

    parser = argparse.ArgumentParser("Backfill new station data.")
    parser.add_argument(
        "-s",
        "--stations",
        nargs="+",
        help="The stations to backfill. If more than one station is being backfilled, separate names with a space.",
    )
    parser.add_argument(
        "--check-backfill",
        action="store_true",
        help="Check station record dates and backfill stations with dates > 2020-01-01 or missing records",
    )
    args = parser.parse_args()
    
    # Validate arguments
    if not args.check_backfill and not args.stations:
        parser.error("Either --stations or --check-backfill must be specified")
    
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
        if args.check_backfill:
            check_and_backfill(session, conn)
        else:
            execute_backfill(args.stations, session, conn)

    finally:
        session.logout()
        conn.close()