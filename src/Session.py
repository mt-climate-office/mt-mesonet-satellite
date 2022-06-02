from dataclasses import dataclass, field
import requests
import subprocess
from pathlib import Path
from typing import Dict, Optional, Tuple, List


@dataclass
class Session:
    username: Optional[str] = None
    password: Optional[str] = None
    creds: Dict[str, str] = field(init=False)
    token: str = field(init=False)

    def __post_init__(self):
        self.creds = self._login(self.username, self.password)
        self.token = self.creds["token"]

    @staticmethod
    def _get_auth() -> Tuple[str]:

        username = (
            subprocess.check_output(
                """awk '/login/ { print $2 }' ~/.netrc""", shell=True
            )
            .strip()
            .decode()
            .split("@")[0]
        )

        password = (
            subprocess.check_output(
                """awk '/password/ { print $2 }' ~/.netrc""", shell=True
            )
            .strip()
            .decode()
        )

        return username, password

    def _login(
        self, username: Optional[str] = None, password: Optional[str] = None
    ) -> Dict[str, str]:

        if not username or not password:
            assert (
                Path.home() / ".netrc"
            ).exists(), "If you don't provide an Earthdata Search login, you must have your login credentials stored in the ~/.netrc file."
            username, password = self._get_auth()
        response = requests.post(
            "https://appeears.earthdatacloud.nasa.gov/api/login",
            auth=(username, password),
        )

        assert (
            response.status_code == 200
        ), "Invalid status code. Please double-check your credentials."

        creds = response.json()
        return creds

    def logout(self):
        response = requests.post(
            "https://appeears.earthdatacloud.nasa.gov/api/logout",
            headers={"Authorization": "Bearer {0}".format(self.token)},
        )

        assert (
            response.status_code == 204
        ), "Invalid HTTP response code. Please check credentials."
