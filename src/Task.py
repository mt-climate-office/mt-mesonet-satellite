from dataclasses import dataclass, field

from Geom import Point, Poly
from typing import Dict, List, Optional, Union
import requests
from pathlib import Path


class PendingTaskError(Exception):
    """Raised when download is attempted on running task."""

    def __init__(self, message="Task still running. Try downloading again later."):
        self.message = message
        super().__init__(self.message)


class InvalidRequestError(Exception):
    """Raised when request response isn't 200."""

    def __init__(self, message="Invalid Request."):
        self.message = message
        super().__init__(self.message)


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
        if response.status_code != 202:
            raise InvalidRequestError(message=task_response['message'])
        self.task_id = task_response["task_id"]
        self.status = task_response["status"]

    def status_update(self, token: str) -> str:
        # TODO: Adjust this to work when task is running. 
        response = requests.get(
            "https://appeears.earthdatacloud.nasa.gov/api/status/{0}".format(
                self.task_id
            ),
            headers={"Authorization": "Bearer {0}".format(token)},
        )

        status_response = response.json()
        if response.status_code == 200 and "status" not in status_response:
            return "pending"
        self.status = status_response["status"]
        return self.status

    def _write_file(self, f: Union[Path, str], dirname: Union[Path, str], token: str):
        response = requests.get(
            "https://appeears.earthdatacloud.nasa.gov/api/bundle/{0}/{1}".format(
                self.task_id, f["file_id"]
            ),
            headers={"Authorization": "Bearer {0}".format(token)},
            allow_redirects=True,
            stream=True,
        )

        pth = Path(dirname) / f["file_name"]
        pth.touch()
        with open(pth, "wb") as con:
            for data in response.iter_content(chunk_size=8192):
                con.write(data)

    def download(self, dirname, token, download_all=False):
        if self.status_update(token) != "done":
            raise PendingTaskError()

        response = requests.get(
            "https://appeears.earthdatacloud.nasa.gov/api/bundle/{0}".format(
                self.task_id
            ),
            headers={"Authorization": "Bearer {0}".format(token)},
        )

        bundle_response = response.json()

        if download_all:
            for f in bundle_response["files"]:
                self._write_file(f, dirname, token)
        else:
            f_list = [x for x in bundle_response["files"] if x["file_type"] == "csv"]
            for f in f_list:
                self._write_file(f, dirname, token)
