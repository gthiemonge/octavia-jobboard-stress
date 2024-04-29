import select
import subprocess
import threading

import psutil

import utils

logger = utils.config_logger(__name__)


_worker_id = 1
def worker_id():
    global _worker_id
    w_id = _worker_id
    _worker_id += 1
    return w_id

class OctaviaWorker:

    def __init__(self, event_queue=None):
        self.name = f"octavia-worker{worker_id()}"
        logger.info(f"Creating {self.name}")
        self.thread = threading.Thread(target=self.run)
        self.event = threading.Event()
        self.process = None
        self.queue = event_queue
        self.logger = utils.config_logger(
            self.name,
            filename=f"{self.name}.log",
            formatter="%(asctime)s %(name)s %(message)s")

    def run(self):
        p = subprocess.Popen("octavia-worker",
                             text=True,
                             stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE)
        self.process = p
        fd_out = p.stdout.fileno()
        fd_err = p.stderr.fileno()
        fds = {
            fd_out: (p.stdout, "OUT"),
            fd_err: (p.stderr, "ERR"),
        }

        poll = select.poll()
        poll.register(fd_out)
        poll.register(fd_err)

        while not self.event.is_set():
            for fd, ev in poll.poll():
                if (ev & select.POLLIN) != 0:
                    fp, tag = fds[fd]
                    for line in fp:
                        line = line.rstrip()
                        escaped_line = utils.ansi_escape.sub('', line)
                        self.queue.put((self.name, escaped_line))
                        self.logger.debug(f"{tag} {line}")
                elif (ev & select.POLLHUP) != 0:
                    break
        logger.debug("Finishing thread")

    def start(self):
        self.thread.start()

    def join(self):
        logger.debug("Joining")
        self.event.set()
        self.thread.join()
        logger.debug("Joined")

    def kill(self):
        logger.info(f"Killing {self.name}")
        pid = self.process.pid

        try:
            for child in psutil.Process(pid).children(recursive=True):
                logger.info(f"Killing child {child.pid}")
                child.kill()
        except psutil.NoSuchProcess:
            pass
        self.process.kill()

        logger.info(f"Killed {self.name}")
