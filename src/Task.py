from dataclasses import dataclass, field

from geoms import Point, Poly
from typing import Dict, List, Optional, Union
import requests


@dataclass
class Task:
    name: str
    products: List[str]
    layers: List[str]
    start_date: str
    end_date: str
    geom: Union[Point, Poly]
    recurring: Optional[bool] = False
    start_year: Optional[int] = None
    end_year: Optional[int] = None
    task_id: str = field(init=False)
    status: str = field(init=False)

    def build_point_task(self) -> Dict[str, str]:
        dates = {
            "startDate": self.start_date,
            "endDate": self.end_date,
        }
        if self.recurring:
            assert (
                self.start_year and self.end_year
            ), "If recurring is set to true, start_year and end_year must also be valid years."
            dates.update(
                {self.recurring: True, "yearRange": [self.start_year, self.end_year]}
            )

        task = {
            "task_type": "point",
            "task_name": self.name,
            "params": {
                "dates": [dates],
                "layers": [
                    {"product": x, "layer": y}
                    for x, y in zip(self.products, self.layers)
                ],
                "coordinates": self.geom.task_format(),
            },
        }

        return task

    def build_poly_task(self) -> Dict[str, str]:
        raise NotImplementedError("This method is not implemented yet.")

    def launch(self, token: str):
        task = (
            self.build_point_task()
            if isinstance(self.geom, Point)
            else self.build_poly_task()
        )

        response = requests.post(
            "https://appeears.earthdatacloud.nasa.gov/api/task",
            json=task,
            headers={"Authorization": "Bearer {0}".format(token)},
        )

        task_response = response.json()
        self.task_id = task_response["task_id"]
        self.status = task_response["status"]

    def status_update(self, token):
        response = requests.get(
            "https://appeears.earthdatacloud.nasa.gov/api/status/{0}".format(
                self.task_id
            ),
            headers={"Authorization": "Bearer {0}".format(token)},
        )
        status_response = response.json()
        self.status = status_response["status"]
