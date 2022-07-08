from __future__ import annotations
from dataclasses import dataclass, field

from .Geom import Point, Poly
from typing import Any, Dict, List, Optional, Union
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


def list_task(token: str) -> List[Dict[str, Any]]:
    """List both completed and currently running tasks.

    Args:
        token (str): Session validation token.

    Returns:
        List[Dict[str, Any]]: List of dicts containing information about all tasks.
    """
    response = requests.get(
        "https://appeears.earthdatacloud.nasa.gov/api/task",
        headers={"Authorization": "Bearer {0}".format(token)},
    )
    task_response = response.json()
    return task_response


@dataclass
class Task:
    """Class to, start, download, and hold information regarding a task

    Raises:
        PendingTaskError: Error raised if download is attempted before task is complete.
    Attributes:
        task_id (Optional[str]): A unique ID associated with a given task.
        status (Optional[str]): The status of the task. One of 'error', 'pending' or 'done'.
    """

    task_id: Optional[str] = None
    status: Optional[str] = None

    @classmethod
    def from_response(cls, response: Dict[str, Any]) -> Task:
        """Create a Task object from a response provided in the list_task function.

        Args:
            response (Dict[str, Any]): Dict obtained from list_tasks.

        Returns:
            Task: Task object generated from the list_task parameters.
        """
        status = "pending" if "status" not in response else response["status"]
        return cls(response["task_id"], status)

    def status_update(self, token: str) -> str:
        """Get an update on the status of the task.

        Args:
            token (str): Token from Session object.

        Returns:
            str: one of 'error', 'pending' or 'done'.
        """
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

    def _write_file(self, f: Dict[str, Any], dirname: Union[Path, str], token: str):
        """Write data from a completed task to disk.

        Args:
            f (Dict[str, Any]): Dictionary containing information about a file.
            dirname (Union[Path, str]): Directory to write the file out to.
            token (str): token from the Session object.
        """
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

    def download(self, dirname: Union[Path, str], token: str, download_all=False):
        """Download all files associated with a task

        Args:
            dirname (Union[Path, str]): Directory to write data to.
            token (str): Token from Session object.
            download_all (bool, optional): Whether or not all associated metadata files should also be saved out. Defaults to False.

        Raises:
            PendingTaskError: Raised if the task is still running.
        """
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
    
    def delete(self, token):
        response = requests.delete(
            'https://appeears.earthdatacloud.nasa.gov/api/task/{0}'.format(self.task_id), 
            headers={'Authorization': 'Bearer {0}'.format(token)}
        )
        return response.status_code


@dataclass
class Submit(Task):
    """Child of Task class that is used to generate a task from various arguments.

    Attributes:
        name (str): The name to associate the task with.
        products (List[str]): List of products to generate a task for.
        layers (List[str]): Layers associated with each product to download.
        start_date (str): "YYYY-MM-DD" formatted string to start task download at.
        end_date (str): "YYYY-MM-DD" formatted string to end task download at.
        geom (Union[Point, Poly]): The geometry to perform the task over.
        recurring (bool): Whether to repeat task downloads over start_year, end_year range. Defaults to False.
        start_year (Optional[int]): Year to start task. Defaults to None.
        end_year (Optional[int]): Year to end task. Defaults to None.

    Raises:
        ValueError: Raised if a hyphen is in the name attribute.
        NotImplementedError: Raised if geom is of type Poly.
        InvalidRequestError: Raised if there are any errors in the request parameters.

    """

    name: str = field(default=str)
    products: List[str] = field(default_factory=list)
    layers: List[str] = field(default_factory=list)
    start_date: str = field(default=str)
    end_date: str = field(default=str)
    geom: Union[Point, Poly] = field(default_factory=Point.from_mesonet)
    recurring: Optional[bool] = False
    start_year: Optional[int] = None
    end_year: Optional[int] = None

    def __post_init__(self):
        if "-" in self.name:
            raise ValueError(
                "A '-' is present in the name arguement. This character cannot be used in the task name."
            )

    def build_point_task(self) -> Dict[str, str]:
        """Build a task for a point geometry.

        Returns:
            Dict[str, str]: Dict of request parameters necessary to run the task.
        """
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
        """Function to begin the task.

        Args:
            token (str): Token from the session object.

        Raises:
            InvalidRequestError: Raised if there are any errors in the request parameters.
        """
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
            raise InvalidRequestError(message=task_response["message"])
        self.task_id = task_response["task_id"]
        self.status = task_response["status"]
