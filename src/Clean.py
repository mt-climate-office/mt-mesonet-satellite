from dataclasses import dataclass, field
import pandas as pd
import numpy as np
from pathlib import Path
from typing import Union, Dict, Optional
from Product import Product, Layer
import janitor


@dataclass
class Cleaner:
    f: Union[str, Path]
    is_subdaily: bool = False
    product: str = field(init=False)
    raw: pd.DataFrame = field(init=False)
    meta: Product = field(init=False)
    layers: Dict[str, Layer] = field(init=False)

    def __post_init__(self):
        self.f = self.f if isinstance(self.f, Path) else Path(self.f)
        parts = self.f.stem.split("-")
        self.product = f"{parts[1]}.{parts[2]}"
        self.raw = pd.read_csv(self.f)
        self.raw.columns = self.raw.columns.str.replace(f"{parts[1]}_{parts[2]}_", "")
        self.meta = Product(self.product)
        self.layers = {
            k:v for k, v in self.meta.layers.items() if k in self.raw.columns
        }
        if self.is_subdaily:
            tmp = {}
            for k, v in self.meta.layers.items():
                for hour in range(0, 24):
                    if f"{k}_{hour}" in self.raw.columns:
                        tmp[f"{k}_{hour}"] = v
            self.layers.update(tmp)

        self.layers = {k: v for k, v in self.layers.items() if not v.IsQA}

    def clean(self):
        dat = self.raw[["ID", "Date"] + list(self.layers.keys())]
            
        for k, v in self.layers.items():
            dat.loc[dat[k] > v.ValidMax, k] = np.nan
            dat.loc[dat[k] < v.ValidMin, k] = np.nan
            dat.loc[dat[k] == v.FillValue, k] = np.nan

        dat = dat.pivot_longer(index=["ID", "Date"], names_to="element")

        if self.is_subdaily:
            dat = self._clean_subdaily(dat)
        
        dat = dat.assign(product = self.product)

        return dat

    @staticmethod
    def _clean_subdaily(dat, to_daily=True):
        dat = dat.assign(spl = dat.element.str.split("_"))
        dat = dat.assign(hour = dat.spl.str[-1])
        dat = dat.assign(element = dat.spl.str[:-1].str.join("_"))
        dat = dat.drop(columns="spl")

        if to_daily:
            dat = dat.groupby_agg(
                by=['ID', 'Date', 'element'],
                agg='mean',
                agg_column_name='value',
                new_column_name='value',
                dropna=True
            ).drop(columns='hour').drop_duplicates().reset_index(drop=True)
            return dat

        hours = set([int(x) for x in dat.hour.to_list()])
        hours = int(24/len(hours))
        dat = dat.assign(hour = dat.hour * hours)
        dat = dat.deconcatenate_column("Date", "-", new_column_names=["year", 'month', 'day'])
        dat = dat.assign(Date = pd.to_datetime(dict(year=dat.year, month=dat.month, day=dat.day, hour=dat.hour)))
        dat = dat.drop(columns=["hour", "year", "month", "day"])
        return dat


def clean_all(dirname: Union[str, Path], save: Optional[Union[str, Path]] = None):
    dirname = dirname if isinstance(dirname, Path) else Path(dirname)
    dfs = []
    for f in dirname.iterdir():
        print(f)
        subdaily = "SPL4SMGP" in f.stem
        c = Cleaner(f, is_subdaily=subdaily)
        tmp = c.clean()
        dfs.append(tmp)
    dat = pd.concat(dfs, axis=0)
    if save:
        dat.to_csv(save)
    return dat
