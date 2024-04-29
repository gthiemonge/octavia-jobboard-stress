import datetime
import random
import queue
import time

import openstack

from event_receiver import EventReceiver
from octavia_worker import OctaviaWorker
import utils


logger = utils.config_logger("octavia-jobboard-stress")


def main():
    q = queue.Queue()

    ev = EventReceiver(event_queue=q)

    workers = {}

    ev.start()

    conn = openstack.connect()

    public_subnet = conn.network.find_subnet("public-subnet")

    lb_index = 1

    next_worker_creation = datetime.datetime.now()
    next_worker_termination = datetime.datetime.now() + utils.randtimedelta()
    next_lb_creation = datetime.datetime.now() + utils.randtimedelta()
    next_lb_deletion = datetime.datetime.now() + utils.randtimedelta()

    start_time = datetime.datetime.now()
    end_time = start_time + datetime.timedelta(hours=1)

    done = False

    try:
        while True:
            now = datetime.datetime.now()
            if now > end_time:
                done = True

            if done:
                try:
                    lbs = list(conn.load_balancer.load_balancers())
                except openstack.exceptions.HttpException:
                    logger.exception("cannot get load balancer list")
                    time.sleep(5)
                    continue
                if len(lbs) == 0:
                    break

            if now > next_worker_creation:
                if len(workers) < 3:
                    w = OctaviaWorker(event_queue=q)
                    w.start()
                    workers[w.name] = w
                next_delta = utils.randtimedelta()
                logger.debug(f"Next worker creation scheduled in {next_delta}")
                next_worker_creation = now + next_delta

            if now > next_worker_termination:
                active_workers = set(ev.get_active_workers())
                active_workers &= workers.keys()
                if len(active_workers) > 0:
                    try:
                        termination_count = random.randint(1, len(workers))
                        terminating_workers = list(active_workers)
                        random.shuffle(terminating_workers)
                        for w in terminating_workers[:termination_count]:
                            workers[w].kill()
                            workers[w].join()
                            workers.pop(w)
                    except:
                        logger.exception("")
                next_delta = utils.randtimedelta()
                logger.debug(f"Next worker termination scheduled in {next_delta}")
                next_worker_termination = now + next_delta

            if now > next_lb_creation and not done:
                try:
                    lbs = list(conn.load_balancer.load_balancers())
                except openstack.exceptions.HttpException:
                    logger.exception("cannot get load balancer list")
                    time.sleep(5)
                    continue
                if len(lbs) < 4:
                    logger.info(f"Creating load balancer lb{lb_index}")
                    conn.load_balancer.create_load_balancer(
                        name=f"lb{lb_index}",
                        vip_subnet_id=public_subnet.id)
                    lb_index += 1
                next_delta = utils.randtimedelta()
                logger.debug(f"Next loadbalancer creation scheduled in {next_delta}")
                next_lb_creation = now + next_delta

            if now > next_lb_deletion:
                try:
                    lbs = list(conn.load_balancer.load_balancers())
                except openstack.exceptions.HttpException:
                    logger.exception("cannot get load balancer list")
                    time.sleep(5)
                    continue
                if len(lbs):
                    to_delete_lbs = []
                    for lb in lbs:
                        if lb.provisioning_status in ('ACTIVE', 'ERROR'):
                            to_delete_lbs.append(lb)
                    if to_delete_lbs:
                        to_delete_lb = random.choice(to_delete_lbs)
                        logger.info(f"Deleting load balancer {lb.name}")
                        conn.load_balancer.delete_load_balancer(to_delete_lb.id)
                next_delta = utils.randtimedelta()
                logger.debug(f"Next loadbalancer deletion scheduled in {next_delta}")
                next_lb_deletion = now + next_delta

            time.sleep(1)
    except KeyboardInterrupt:
        pass
    except Exception:
        logger.exception("")

    for w in workers.values():
        w.kill()
    for w in workers.values():
        w.join()
    ev.kill()
    ev.join()

if __name__ == "__main__":
    main()