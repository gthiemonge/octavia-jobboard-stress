import re
import threading

import utils

logger = utils.config_logger(__name__)


class EventReceiver:
    flow_re = re.compile(
        r"Flow '(?P<action>[A-Za-z_-]*)[_-]flow(.*)' "
        r"\((?P<job_id>[a-z0-9-]*)\) "
        r"transitioned into state '(?P<new_state>[A-Z]*)' "
        r"from state '(?P<from_state>[A-Z]*)'")

    def __init__(self, event_queue):
        self.thread = threading.Thread(target=self.run)
        self.queue = event_queue
        self.data = {"workers": {}, "jobs": {}}
        self.data_lock = threading.Lock()
        self.event = threading.Event()

    def run(self):
        while not self.event.is_set():
            ev = self.queue.get()
            if ev:
                self.process_event(*ev)

    def process_event(self, source, event):
        m = self.flow_re.search(event)
        if m:
            d = m.groupdict()

            if source not in self.data["workers"]:
                self.data["workers"][source] = {
                    "busy": 0
                }
            worker_status = self.data["workers"][source]

            job_id = d["job_id"]
            job = self.data["jobs"].get(job_id, None)

            states = (d["new_state"], d["from_state"])

            if states == ("RUNNING", "PENDING"):
                state = "Started"
                worker_status["busy"] += 1
            elif states == ("SUCCESS", "RUNNING"):
                state = "Complete"
                worker_status["busy"] -= 1
            elif states == ("RESUMING", "RUNNING"):
                state = "Resumed"
                worker_status["busy"] += 1
            elif (states in (("SUSPENDED", "RESUMING"),
                             ("RUNNING", "SUSPENDED"))):
                return
            else:
                state = f"Unknown ({states})"

            if not job:
                job = {}
                self.data["jobs"][job_id] = job

            job["job_state"] = state
            job["owner"] = source

            logger.info(f"Job {job_id} {state} on {source}")

    def get_active_workers(self):
        return [
            w
            for w, stats in self.data["workers"].items()
            if stats["busy"] > 0
        ]

    def start(self):
        self.thread.start()

    def join(self):
        return self.thread.join()

    def kill(self):
        self.event.set()
        self.queue.put(None)
        logger.debug(self.data)
