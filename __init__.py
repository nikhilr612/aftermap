"""
A small and simple library to make applying expensive functions resumable from disruption.
"""
from worker import PullWorker, TInput, TOutput, WorkerFunction
from typing import Generic, Iterable
from writer import Writer

class AfterMap(Generic[TInput, TOutput]):
    """
    Class to store settings, and spawn processes.     
    """

    def roundrobin(self, fn: WorkerFunction[TInput, TOutput], it: Iterable[TInput]):
        # TODO: Resume logic; check logs to find out which instances have completed execution
        #       Consider moving resume logic to its own function.
        #       Also, find out how many runs have been done already and set run id to that number (so no runs -> runid = 0).
        #       By default, an instance is not said to have completed.
        #       If an instance has a FAILURE log or an instance id is present in the persitent results table, then that instance is said to have completed.
        # TODO: Start consumer, Bind ZMQ Push socket
        # TODO: Start all workers
        # All configuration details should be stored in `AfterMap` class as fields.
        for instance_id, inp in enumerate(it):
            # TODO: Check if complted (maybe use a set / hashmap built before?). Yes? - then skip.
            # TODO: Push to ZMQ socket
            pass
        # for i in range(num workers):
        #  send dismiss command
        # Now we wait for all workers to finish
        # Send termination command to consumer
        pass
