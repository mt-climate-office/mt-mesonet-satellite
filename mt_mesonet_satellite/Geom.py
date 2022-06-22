from __future__ import annotations
from dataclasses import dataclass
import pandas as pd
from typing import List, Dict, Union
import geopandas as gpd
from pathlib import Path


@dataclass
class Point:
    """Class to encode point data into json that can be accepted by the AppEEARS API.

    Attributes:
        lats (List[float]): List of latitudes to extract data at.
        lons (List[float]): List of longitudes to extract data at.
        ids (List[str]): List of unique IDs to identify each point.

    """

    lats: List[float]
    lons: List[float]
    ids: List[str]

    def task_format(self) -> List[Dict[str, Union[int, str]]]:
        """Format the latitudes, longitudes and ids into a format accepted by AppEEARS API.

        Returns:
            List[Dict[str, Union[int, str]]]: List of lat, lon, id dictionaries.
        """
        return [
            {"latitude": x, "longitude": y, "id": z}
            for x, y, z in zip(self.lats, self.lons, self.ids)
        ]

    @classmethod
    def from_mesonet(cls) -> Point:
        """Return Point class for Montana Mesonet stations.

        Returns:
            Point: Point class of Mesonet stations.
        """
        dat = pd.read_csv("https://mesonet.climate.umt.edu/api/v2/stations/?type=csv")
        return cls(
            lats=dat.latitude.tolist(),
            lons=dat.longitude.tolist(),
            ids=dat.station.tolist(),
        )

    @classmethod
    def from_geojson(cls, pth: Union[Path, str], id_col: str) -> Point:
        """Read a .geojson file and convert it into a Point object.

        Args:
            pth (Union[Path, str]): Path to the .geojson to convert.
            id_col (str): The .geojson attribute column to use as the Point's ID attribute.

        Returns:
            Point: A Point object from the
        """
        dat = gpd.read_file(pth)
        return cls(
            lats=dat.geometry.y.tolist(),
            lons=dat.geometry.x.tolist(),
            ids=dat[id_col].tolist(),
        )


@dataclass
class Poly:
    pass
    # raise NotImplementedError("This class is not implemented yet")
