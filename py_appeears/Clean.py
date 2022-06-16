from dataclasses import dataclass, field
import pandas as pd
import numpy as np
from pathlib import Path
from typing import Union, Dict, Optional
from .Product import Product, Layer
import janitor


@dataclass
class Cleaner:
    """Class to clean .csv returned by AppEEARS API

    Attributes:
        f (Union[str, Path]): Path to the .csv file to clean.
        is_subdaily (bool): Whether or not the product has sub-daily observations.
        product (str): The product name. Derived from the filename.
        raw (pd.DataFrame): The raw data from the .csv. Derived from the filename.
        meta (Product): Product object providing metadata. Derived from filename.
        layers (Dict[str, Layer]): Dict of layer objects associated with a product. Derived from filename.
    """

    f: Union[str, Path]
    is_subdaily: bool = False
    product: str = field(init=False)
    raw: pd.DataFrame = field(init=False)
    meta: Product = field(init=False)
    layers: Dict[str, Layer] = field(init=False)

    def __post_init__(self):
        self.f = self.f if isinstance(self.f, Path) else Path(self.f)
        parts = self.f.stem.split("-")
        self.product = f"{parts[-3]}.{parts[-2]}"
        self.raw = pd.read_csv(self.f)
        self.raw.columns = self.raw.columns.str.replace(f"{parts[-3]}_{parts[-2]}_", "")
        self.meta = Product(self.product)
        self.layers = {
            k: v for k, v in self.meta.layers.items() if k in self.raw.columns
        }
        if self.is_subdaily:
            tmp = {}
            for k, v in self.meta.layers.items():
                for hour in range(0, 24):
                    if f"{k}_{hour}" in self.raw.columns:
                        tmp[f"{k}_{hour}"] = v
            self.layers.update(tmp)

        self.layers = {k: v for k, v in self.layers.items() if not v.IsQA}

    def clean(self) -> pd.DataFrame:
        """Removes invalid data and fills with NA. Pivots from wide to long format.

        Returns:
            pd.DataFrame: DataFrame of cleaned data.
        """
        dat = self.raw[["ID", "Date"] + list(self.layers.keys())]

        for k, v in self.layers.items():
            dat.loc[dat[k] > v.ValidMax, k] = np.nan
            dat.loc[dat[k] < v.ValidMin, k] = np.nan
            dat.loc[dat[k] == v.FillValue, k] = np.nan

        dat = dat.pivot_longer(index=["ID", "Date"], names_to="element")

        if self.is_subdaily:
            dat = self._clean_subdaily(dat)

        dat = dat.assign(product=self.product)

        unit_map = {k:v.Units for k, v in self.layers.items()}
        dat = dat.assign(units = dat['element'])
        dat = dat.replace({'units': unit_map})

        return dat

    @staticmethod
    def _clean_subdaily(dat: pd.DataFrame, to_daily: bool = True) -> pd.DataFrame:
        """Cleans subdaily data as they are formatted rather strangely by AppEEARS. Can optionally aggregate to daily means.

        Args:
            dat (pd.DataFrame): DataFrame of sub-daily AppEEARS data.
            to_daily (bool, optional): Wheteher or not to aggregate the data to a daily mean. Defaults to True.

        Returns:
            pd.DataFrame: Cleaned sub-daily dataframe.
        """
        dat = dat.assign(spl=dat.element.str.split("_"))
        dat = dat.assign(hour=dat.spl.str[-1])
        dat = dat.assign(element=dat.spl.str[:-1].str.join("_"))
        dat = dat.drop(columns="spl")

        if to_daily:
            dat = (
                dat.groupby_agg(
                    by=["ID", "Date", "element"],
                    agg="mean",
                    agg_column_name="value",
                    new_column_name="value",
                    dropna=True,
                )
                .drop(columns="hour")
                .drop_duplicates()
                .reset_index(drop=True)
            )
            return dat

        hours = set([int(x) for x in dat.hour.to_list()])
        hours = int(24 / len(hours))
        dat = dat.assign(hour=dat.hour * hours)
        dat = dat.deconcatenate_column(
            "Date", "-", new_column_names=["year", "month", "day"]
        )
        dat = dat.assign(
            Date=pd.to_datetime(
                dict(year=dat.year, month=dat.month, day=dat.day, hour=dat.hour)
            )
        )
        dat = dat.drop(columns=["hour", "year", "month", "day"])
        return dat


def clean_all(
    dirname: Union[str, Path], save: Optional[Union[str, Path]] = None
) -> pd.DataFrame:
    """Clean all of the AppEEARS .csv files in a directory and combine them into a single dataframe.

    Args:
        dirname (Union[str, Path]): Directory containing the files to clean.
        save (Optional[Union[str, Path]], optional): Pathname to save file to. If left as none, the file is not saved, but the dataframe is returned. Defaults to None.

    Returns:
        pd.DataFrame: DataFrame of combined and cleaned data.
    """
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
        dat.to_csv(save, index=False)
    return dat
