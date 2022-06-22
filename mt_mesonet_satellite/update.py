import pandas as pd
import datetime as dt
from typing import List, Union
from pathlib import Path
import time
from operator import itemgetter

from .Task import Submit, PendingTaskError
from .Session import Session
from .Geom import Point
from .Clean import clean_all


def find_missing_data(dat: pd.DataFrame) -> pd.DataFrame:
    """Looks for the last timestamp for each product and returns the information in a dataframe
    Args:
        dat (pd.DataFrame): DataFrame containing all AppEEARS data.

    Returns:
        pd.DataFrame: DataFame of dates of the last timestamp of each product.
    """
    dat = dat[["ID", "Date", "element", "product"]].drop_duplicates()
    dat = dat.groupby(["product", "element"]).agg({"Date": "max"}).reset_index()
    dat = dat.assign(Date=pd.to_datetime(dat.Date).dt.date)
    return dat


def start_missing_tasks(
    session: Session, master_db: pd.DataFrame, start_now: bool = True
) -> List[Submit]:
    """Finds the last data downloaded for each product and starts tasks to fill the missing data.

    Args:
        session (Session): Session object with login credentials.
        master_db (pd.DataFrame): DataFrame with all AppEEARS data.
        start_now (bool, optional): Whether the tasks should be started before being returned. Defaults to True.

    Returns:
        List[Submit]: List of tasks that will fill missing data.
    """
    missing = find_missing_data(master_db)
    products = list(set(missing["product"]))
    geom = Point.from_mesonet()
    tasks = []
    for p in products:
        sub = missing[missing["product"] == p].reset_index(drop=True)
        assert (
            len(sub["Date"].drop_duplicates()) == 1
        ), "There are more than two start_dates. You didn't implement anything to deal with this..."

        date = sub["Date"][0]
        today = dt.date.today()
        if (today - date).days < 7:
            print("Old images are less than a week old. Not updating.")
            continue

        start_date = str(date)
        end_date = str(today)
        date = str(date).replace("-", "")
        today = str(today).replace("-", "")
        task_name = f"{p}_{date}_{today}"
        task = Submit(
            name=task_name,
            products=sub["product"].tolist(),
            layers=sub["element"].tolist(),
            start_date=start_date,
            end_date=end_date,
            geom=geom,
        )
        tasks.append(task)

    if start_now:
        [task.launch(session.token) for task in tasks]

    return tasks


def wait_on_tasks(
    tasks: List[Submit], session: Session, dirname: Union[str, Path], wait: int = 300
) -> None:
    """Wait until all tasks are completed and download the data once they are.

    Args:
        tasks (List[Submit]): A list of running tasks.
        session (Session): Session object with login credentials.
        dirname (Union[str, Path]): Directory to save results out to.
        wait (int, optional): How long to wait before trying to download data again. Defaults to 300.
    """
    indices = []
    while True:
        for idx, task in enumerate(tasks):
            try:
                task.download(dirname, session.token, False)
                tasks.pop(idx)
            except PendingTaskError as e:
                print(f"{e}\n{task.name} is still running...")
                indices.append(idx)

        try:
            getter = itemgetter(*indices)
            tasks = [*getter(tasks)]
        except TypeError as e:
            print("All tasks have completed.")
            break

        print(f"Waiting {wait} seconds to try again...")
        time.sleep(wait)


def update_master(dirname: Union[str, Path], master: Union[str, Path]) -> pd.DataFrame:
    """Update the master dataframe with recent observations.

    Args:
        dirname (Union[str, Path]): Directory contatining the recent observations.
        master (Union[str, Path]): Path to the master dataframe.

    Returns:
        pd.DataFrame: Updated version of the master dataframe.
    """
    dat = clean_all(dirname, None)
    master_df = pd.read_csv(master)
    out = pd.concat([dat, master_df], axis=0).drop_duplicates()
    out.to_csv(master, index=False)
    return out
