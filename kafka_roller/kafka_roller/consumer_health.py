"""Kafka consumer util methods"""
import argparse
import sys
import time

import requests


def consumers_are_healthy(burrow, consumers, retries, retry_wait):
    """Return true if all the consumers are healthy"""
    print("Checking health of {}".format(", ".join(consumers)))
    retries_left = retries
    while retries_left:
        if all(consumer_is_healthy(burrow, c) for c in consumers):
            print("All consumers are healthy!")
            return True
        else:
            print(
                "Will retry in {} seconds, retries left/total = {}/{}.".format(
                    retry_wait, retries_left, retries
                )
            )
            time.sleep(retry_wait)
            retries_left = retries_left - 1
    print("Consumers found to be un-healthy after multiple retries.")
    return False


def consumer_is_healthy(burrow, consumer):
    """Return true if consumer is healthy"""
    res = requests.get("{}/consumer/{}/status".format(burrow, consumer))
    if res.status_code != 200:
        print("Did not find consumer {}, aborting!".format(consumer))
        sys.exit(1)
    all_good = res.json().get("status", {}).get("status", None) == "OK"
    if not all_good:
        print("Consumer {} is not healthy!".format(consumer))
    return all_good


def main():
    parser = argparse.ArgumentParser(description="Check health of consumers.")
    parser.add_argument(
        "burrow", help="burrow endpoint, ex: http://localhost:8000/v3/kafka/mycluster"
    )
    parser.add_argument(
        "consumers", metavar="consumer", nargs="+", help="a kafka consumer"
    )
    parser.add_argument(
        "-r",
        type=int,
        default=10,
        help="number of health check retries before aborting",
    )
    parser.add_argument(
        "-w", type=int, default=30, help="seconds to wait between each retry."
    )
    args = parser.parse_args()
    try:
        requests.get(args.burrow)
    except requests.exceptions.RequestException:
        print("Burrow at {} is not reachable, aborting!".format(args.burrow))
        sys.exit(1)
    sys.exit(0) if consumers_are_healthy(
        args.burrow, args.consumers, args.r, args.w
    ) else sys.exit(1)
