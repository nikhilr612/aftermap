"""
A small and simple library to make applying expensive functions resumable from disruption.
"""

__version__ = "0.1.0"

import time
import zmq
from functools import partial

from worker import (
    PullWorker,
    RunnerContext,
    TInput,
    TOutput,
    WorkerFunction,
    LoadCommand,
    DISPATCH_COMPLETE,
    adapter as worker_adapter,
)
from typing import Iterable, Self, Callable
from writer import Writer, adapter as writer_adapter, TermCommand
import logging

logger = logging.getLogger(__name__)


def _wrapper(
    func: Callable[[TInput], TOutput], ctx: RunnerContext, inp: TInput
) -> TOutput:
    return func(inp)


class AfterMapBuilder:
    """
    Builder for configuring `AfterMap` settings.
    Use `AfterMap.builder()` to create.
    """

    def __init__(self):
        self._num_workers = 2
        self._worker_startup_timeout = 5.0
        self._writer_bind_delay = 0.5
        self._inter_job_delay = 0.00
        self._socket_linger = 0.1
        self._zmq_job_addr = "tcp://127.0.0.1:5554"
        self._zmq_result_addr = "tcp://127.0.0.1:5555"
        self._zmq_control_addr = "tcp://127.0.0.1:5553"
        self._job_queue_timeout = 4.0
        self._min_insert_batch_logs = 32
        self._min_insert_batch_results = 8
        self._flush_interval = 60

    def num_workers(self, n: int) -> Self:
        """Number of worker processes (default: 2)"""
        self._num_workers = n
        return self

    def worker_startup_timeout(self, t: float) -> Self:
        """Wait time for workers to connect (default: 5.0s)"""
        self._worker_startup_timeout = t
        return self

    def writer_bind_delay(self, d: float) -> Self:
        """Wait time for writer to bind socket (default: 0.5s)"""
        self._writer_bind_delay = d
        return self

    def inter_job_delay(self, d: float) -> Self:
        """Artificial delay between job sends to mitigate ZMQ fair-queue issues.

        ZMQ's PUSH socket returns immediately after queuing, not after transmission.
        Without delay, all jobs may be queued to the same worker before fair-queue
        can distribute them. Default is 0.00s (no delay). Increase if jobs are
        sent faster than workers can process them.
        """
        self._inter_job_delay = d
        return self

    def socket_linger(self, s: float) -> Self:
        """Socket close linger time in seconds (default: 0.1)"""
        self._socket_linger = s
        return self

    def zmq_job_addr(self, addr: str) -> Self:
        """ZMQ address for job dispatch (default: tcp://127.0.0.1:5554)"""
        self._zmq_job_addr = addr
        return self

    def zmq_result_addr(self, addr: str) -> Self:
        """ZMQ address for result collection (default: tcp://127.0.0.1:5555)"""
        self._zmq_result_addr = addr
        return self

    def zmq_control_addr(self, addr: str) -> Self:
        """ZMQ address for control messages (default: tcp://127.0.0.1:5553)"""
        self._zmq_control_addr = addr
        return self

    def job_queue_timeout(self, s: float) -> Self:
        """Worker job queue receive timeout in seconds (default: 4.0)"""
        self._job_queue_timeout = s
        return self

    def min_insert_batch_logs(self, n: int) -> Self:
        """Min log batch size before flush (default: 32)"""
        self._min_insert_batch_logs = n
        return self

    def min_insert_batch_results(self, n: int) -> Self:
        """Min result batch size before flush (default: 8)"""
        self._min_insert_batch_results = n
        return self

    def flush_interval(self, s: float) -> Self:
        """Max time between flushes in seconds (default: 60)"""
        self._flush_interval = s
        return self

    def build(self) -> "AfterMap":
        return AfterMap(
            num_workers=self._num_workers,
            worker_startup_timeout=self._worker_startup_timeout,
            writer_bind_delay=self._writer_bind_delay,
            inter_job_delay=self._inter_job_delay,
            socket_linger=self._socket_linger,
            zmq_job_addr=self._zmq_job_addr,
            zmq_result_addr=self._zmq_result_addr,
            zmq_control_addr=self._zmq_control_addr,
            job_queue_timeout=self._job_queue_timeout,
            min_insert_batch_logs=self._min_insert_batch_logs,
            min_insert_batch_results=self._min_insert_batch_results,
            flush_interval=self._flush_interval,
        )


class AfterMap:
    """
    Main entry point for aftermap.
    Use AfterMap.builder() to configure, then run() or maplike() to execute.
    """

    @classmethod
    def builder(cls) -> AfterMapBuilder:
        """Create a builder for configuring AfterMap settings."""
        return AfterMapBuilder()

    def results(self, dbpath: str, output_type: type[TOutput]) -> Iterable[TOutput]:
        """
        An iterator over persistent results in the db.
        """
        from schema import PersistedResult
        import sqlalchemy
        import sqlalchemy.orm
        from sqlalchemy import func

        engine = sqlalchemy.create_engine(f"sqlite:///{dbpath}")
        with sqlalchemy.orm.Session(engine) as session:
            for row in session.execute(
                sqlalchemy.select(
                    PersistedResult.instanceid,
                    func.json(PersistedResult.blob).label("json_data"),
                ).order_by(PersistedResult.instanceid)
            ).all():
                yield output_type.model_validate_json(row.json_data)

    def maplike(
        self, input_type: type[TInput], output_type: type[TOutput], dbpath: str
    ):
        """
        Return an interface similar to the builtin `map`.
        However, unlike the builtin map, the function is applied eagerly and iterable is exhausted.
        Results are yielded after all workers have completed.
        """

        def mapper(
            func: Callable[[TInput], TOutput | None], it: Iterable[TInput]
        ) -> Iterable[TOutput]:
            self.run(dbpath, partial(_wrapper, func), input_type, output_type, it)
            return self.results(dbpath, output_type)

        return mapper

    def __init__(
        self,
        num_workers: int = 2,
        worker_startup_timeout: float = 5.0,
        writer_bind_delay: float = 0.5,
        inter_job_delay: float = 0.01,
        socket_linger: float = 0.1,
        zmq_job_addr: str = "tcp://127.0.0.1:5554",
        zmq_result_addr: str = "tcp://127.0.0.1:5555",
        zmq_control_addr: str = "tcp://127.0.0.1:5553",
        job_queue_timeout: float = 4.0,
        min_insert_batch_logs: int = 32,
        min_insert_batch_results: int = 8,
        flush_interval: float = 60,
    ):
        self.num_workers = num_workers
        self.worker_startup_timeout = worker_startup_timeout
        self.writer_bind_delay = writer_bind_delay
        self.inter_job_delay = inter_job_delay
        self.socket_linger = socket_linger
        self.zmq_job_addr = zmq_job_addr
        self.zmq_result_addr = zmq_result_addr
        self.zmq_control_addr = zmq_control_addr
        self.job_queue_timeout = job_queue_timeout
        self.min_insert_batch_logs = min_insert_batch_logs
        self.min_insert_batch_results = min_insert_batch_results
        self.flush_interval = flush_interval

    def run(
        self,
        dbpath: str,
        fn: WorkerFunction[TInput, TOutput],
        input_type: type[TInput],
        output_type: type[TOutput],
        inputs: Iterable[TInput],
    ) -> None:
        """
        Run the map function across inputs using multiple workers.

        :param dbpath: Path to SQLite database for persistence
        :param fn: Function to apply (RunnerContext, TInput) -> TOutput | None
        :param input_type: Pydantic model type for inputs
        :param output_type: Pydantic model type for outputs
        :param inputs: Iterable of inputs to process
        """
        writer_proc = Writer(
            dbpath=dbpath,
            runid=0,
            zmq_bind_addr=self.zmq_result_addr,
            min_insert_batch_logs=self.min_insert_batch_logs,
            min_insert_batch_results=self.min_insert_batch_results,
            flush_interval=self.flush_interval,
        )
        writer_proc.start()
        time.sleep(self.writer_bind_delay)

        zctx = zmq.Context()
        job_sock = zctx.socket(zmq.PUSH)
        job_sock.bind(self.zmq_job_addr)

        # pub-sub mechanism to notify all workers of dispatcher exit.
        control_sock = zctx.socket(zmq.PUB)
        control_sock.bind(self.zmq_control_addr)

        workers = []
        for i in range(self.num_workers):
            worker = PullWorker(
                id=i,
                runid=0,
                input_type=input_type,
                output_type=output_type,
                fn=fn,
                zmq_job_addr=self.zmq_job_addr,
                zmq_result_addr=self.zmq_result_addr,
                zmq_control_addr=self.zmq_control_addr,
                job_queue_timeout=int(self.job_queue_timeout * 1000),
            )
            worker.start()
            workers.append(worker)

        time.sleep(self.worker_startup_timeout)

        for instance_id, inp in enumerate(inputs):
            cmd = LoadCommand(data=inp.model_dump(), instanceid=instance_id)
            job_sock.send(worker_adapter.dump_json(cmd))
            time.sleep(self.inter_job_delay)

        logger.info("Exhausted instances to map on.")
        # send message to indicate jobs have been exhausted
        control_sock.send_string(DISPATCH_COMPLETE)
        control_sock.close()

        for worker in workers:
            worker.join()

        logger.info("Closing job queue")
        job_sock.close(linger=int(self.socket_linger * 1000))

        writer_sock = zctx.socket(zmq.PUSH)
        writer_sock.connect(self.zmq_result_addr)

        logger.info("Sent kys to writer.")
        writer_sock.send(writer_adapter.dump_json(TermCommand()))
        writer_sock.close(linger=int(self.socket_linger * 1000))

        writer_proc.join(timeout=5)
        zctx.term()
        logger.info("Map finish")
