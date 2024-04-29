import datetime
import logging
import random
import re

ansi_escape = re.compile(r'\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])')


def config_logger(name, filename=None, formatter=None):
    l = logging.getLogger(name)
    l.setLevel(logging.DEBUG)

    if filename is None:
        filename = "octavia-jobboard-stress.log"

    fh = logging.FileHandler(filename)
    fh.setLevel(logging.DEBUG)

    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)

    if formatter is None:
        formatter = '%(asctime)s %(name)s [%(levelname)s] %(message)s'
    formatter = logging.Formatter(formatter)
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)

    l.addHandler(fh)
    l.addHandler(ch)
    return l

def randtimedelta():
    return datetime.timedelta(seconds=random.randint(15, 30))
