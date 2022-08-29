from __future__ import annotations

from dataclasses import dataclass, field, fields
from typing import Dict, List, Optional

import requests


@dataclass
class Layer:
    """Class that contains all the metadata associated with a satellite data product layer."""

    AddOffset: Optional[float]
    Available: bool
    DataType: str
    Description: str
    Dimensions: List[str]
    FillValue: int
    IsQA: bool
    Layer: str
    OrigDataType: str
    OrigValidMax: int
    OrigValidMin: int
    QualityLayers: str
    QualityProductAndVersion: str
    ScaleFactor: Optional[int]
    Units: str
    ValidMax: int
    ValidMin: int
    XSize: int
    YSize: int

    # Credit to: https://stackoverflow.com/a/57208298
    @classmethod
    def from_dict(cls, d) -> Layer:
        cls_fields = {f.name for f in fields(cls)}
        return Layer(**{k: v for k, v in d.items() if k in cls_fields})


@dataclass
class Product:
    """Class to represent a satellite data product

    Attributes:
        product (str): The name of the product to get data for. Formatted as NAMEOFPRODUCT.XXX, where XXX is the product version number.
        layers (Dict[str, Layer]): List of layers associated with a product.
    """

    product: str
    layers: Dict[str, Layer] = field(init=False)

    def __post_init__(self):
        self.layers = self.get_layers()

    def get_layers(self):
        response = requests.get(
            "https://appeears.earthdatacloud.nasa.gov/api/product/{0}".format(
                self.product
            )
        )
        layer_response = response.json()

        return {k: Layer.from_dict(v) for k, v in layer_response.items()}
