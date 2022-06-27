import argparse
from pathlib import Path
import pandas as pd
from mt_mesonet_satellite import (
    start_missing_tasks,
    wait_on_tasks,
    update_master,
    Session,
    Point,
)

if __name__ == "__main__":

    parser = argparse.ArgumentParser("CLI Utility to update satellite data")
    parser.add_argument(
        "-f", "--fname", type=Path, help="Path to master dataframe with all data"
    )
    parser.add_argument(
        "-dd", "--datadir", type=Path, help="Base directory to save new data to."
    )

    args = parser.parse_args()

    stations = Point.from_mesonet()
    session = Session()
    dat = pd.read_csv(args.fname)

    tasks = start_missing_tasks(session, dat, True)
    wait_on_tasks(tasks, session, args.datadir, 300)
    update_master(args.datadir, args.fname)
    session.logout()
