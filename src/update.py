import pandas as pd
import datetime as dt

from Task import Submit
from Session import Session
from Geom import Point


def find_missing_data(dat: pd.DataFrame) -> pd.DataFrame:
    dat = dat[['ID', 'Date', 'element', 'product']].drop_duplicates()
    dat = dat.groupby(['product', 'element']).agg({"Date": 'max'}).reset_index()
    dat = dat.assign(Date = pd.to_datetime(dat.Date).dt.date)
    return dat

def get_latest_data(master_db: pd.DataFrame, start_now=True):

    missing = find_missing_data(master_db)
    products = list(set(missing['product']))
    geom = Point.from_mesonet()
    tasks = []
    session = Session()
    for p in products:
        sub = missing[missing['product'] == p].reset_index(drop=True)
        assert len(sub['Date'].drop_duplicates()) == 1, "There are more than two start_dates. You didn't implement anything to deal with this..."
        
        
        date = sub['Date'][0]
        today = dt.date.today()
        if (today - date).days < 7:
            print("Old images are less than a week old. Not updating.")
            continue

        start_date = str(date)
        end_date = str(today)
        date = str(date).replace('-', '')
        today = str(today).replace('-', '')
        task_name = f"{p}_{date}_{today}"
        task = Submit(
            name=task_name,
            products=sub['product'].tolist(),
            layers=sub['product'].tolist(),
            start_date=start_date,
            end_date=end_date,
            geom=geom
        )
        tasks.append(task)
    # TODO: Figure out what isn't working here.
    if start_now:
        [task.launch(session.token) for task in tasks]
