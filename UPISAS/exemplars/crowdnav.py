import time

import requests

from UPISAS.exemplar import Exemplar


class CrowdNav(Exemplar):
    """CrowdNav, reached through its HTTP API.

    The three CrowdNav containers are started with docker compose,
    so this class does not create or start any container.
    """

    def __init__(self):
        # no super().__init__() here, because that would create a new container
        self.base_endpoint = "http://localhost:8080"
        self.exemplar_container = None

    def start_run(self):
        """Wait until the simulation publishes data.

        Right after "docker restart crowdnav" the API still shows the last
        message of the old run. So we wait until the step number goes up,
        which means a simulation is really running.
        """
        last_step = None
        while True:
            data = requests.get(self.base_endpoint + "/monitor/").json()
            if data is not None:
                step = data["car_stats"]["step"]
                if last_step is not None and step > last_step:
                    return
                last_step = step
            time.sleep(1)
