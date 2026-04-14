"""
Basic test to verify aftermap functionality with a local db file.
"""

import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from pydantic import BaseModel
from __init__ import AfterMap


class InputData(BaseModel):
    value: int


class OutputData(BaseModel):
    doubled: int


def slow_fn(data: InputData) -> OutputData:
    time.sleep(0.1)
    return OutputData(doubled=data.value * 3)


def main():
    dbpath = "test.db"
    if os.path.exists(dbpath):
        os.remove(dbpath)

    print("Building AfterMap...")
    am = (
        AfterMap.builder()
        .num_workers(2)
        .worker_startup_timeout(3.0)
        .inter_job_delay(0.01)
        .build()
    )
    print("Creating mapper...")
    mapper = am.maplike(InputData, OutputData, dbpath)
    print("Running function...")
    results = list(mapper(slow_fn, [InputData(value=i) for i in range(6)]))
    print(f"Results: {results}")
    print(f"Total: {len(results)}")


if __name__ == "__main__":
    main()
