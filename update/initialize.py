import argparse
import datetime as dt
import os
import tempfile

from dotenv import load_dotenv
from mt_mesonet_satellite import (
    MesonetSatelliteDB,
    Point,
    Session,
    Submit,
    clean_all,
    to_db_format,
    wait_on_tasks,
)

if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        "Script to initialize the satellite indicators Neo4j database."
    )
    parser.add_argument(
        "-e", "--env", type=str, default=".env", help="Path to your .env file."
    )
    parser.add_argument(
        "-ul",
        "--neo4jpth",
        type=str,
        default="/neo4j/import",
        help="Path to the linked Neo4j 'import' volume.",
    )

    args = parser.parse_args()

    # If your .env file isn't in the current directory, put the path to
    load_dotenv(args.env)

    # Initialize the indices and constraints for the database.
    conn = MesonetSatelliteDB(
        uri=os.getenv("Neo4jURI"),
        user=os.getenv("Neo4jUser"),
        password=os.getenv("Neo4jPassword"),
    )
    conn.init_db_indices()

    # Begin an AppEEARS session.
    session = Session(dot_env=False)

    # Initialize point geometries from mesonet station locations.
    # You can also initialize from a .geojson file with Point.from_geojson("/path/to/file.geojson")
    stations = Point.from_mesonet()

    # Make a Submit request object for VIIRS EVI and NDVI data.
    viirs = Submit(
        name="viirs_backfill",
        products=[
            "VNP13A1.001",
            "VNP13A1.001",
        ],
        layers=[
            "_500_m_16_days_EVI",
            "_500_m_16_days_NDVI",
        ],
        start_date="2020-01-01",
        end_date=str(dt.date.today()),
        geom=stations,
    )

    # Start the request. The processing of this could take a while.
    viirs.launch(session.token)

    with tempfile.TemporaryDirectory() as dirname:
        # Wait for the VIIRS task to complete and save it to the tempdir.
        # In the below function, "wait" is the number of seconds to wait before
        # checking if the task is complete.
        wait_on_tasks(tasks=viirs, session=session, dirname=dirname, wait=3600)
        # Clean the processed data.
        cleaned = clean_all(dirname, False)

        # On linux OS, there can be permissions errors with the linked Docker volumes.
        # If this occurs, manually post the entries to the db (This is slower than using
        # the builtin Neo4j CSV reader).
        try:
            formatted = to_db_format(
                f=cleaned,
                neo4j_pth=args.neo4jpth,
                out_name="data_init",
                write=True,
                split=True,
            )

            # Upload the data to the database using the Neo4j CSV reader.
            conn.init_db(dirname)
        except (FileNotFoundError, PermissionError) as e:
            formatted = to_db_format(
                f=cleaned, neo4j_pth=None, out_name=None, write=False, split=False
            )

            # Upload to database row by row.
            conn.post(formatted)

    session.logout()
    conn.close()
