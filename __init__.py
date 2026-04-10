"""
A small and simple library to make applying expensive functions resumable from disruption.
"""

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
    DismissCommand,
    adapter as worker_adapter,
)
from typing import Iterable, Self, Callable
from writer import Writer, adapter as writer_adapter, TermCommand


def _wrapper(
    func: Callable[[TInput], TOutput], ctx: RunnerContext, inp: TInput
) -> TOutput:
    return func(inp)


class AfterMapBuilder:
    def __init__(self):
        self._num_workers = 2
        self._worker_startup_timeout = 5.0
        self._writer_bind_delay = 0.5
        self._inter_job_delay = 0.01
        self._socket_linger = 100
        self._zmq_job_addr = "tcp://127.0.0.1:5554"
        self._zmq_result_addr = "tcp://127.0.0.1:5555"
        self._min_insert_batch_logs = 32
        self._min_insert_batch_results = 8
        self._flush_interval = 60

    def num_workers(self, n: int) -> Self:
        self._num_workers = n
        return self

    def worker_startup_timeout(self, t: float) -> Self:
        self._worker_startup_timeout = t
        return self

    def writer_bind_delay(self, d: float) -> Self:
        self._writer_bind_delay = d
        return self

    def inter_job_delay(self, d: float) -> Self:
        self._inter_job_delay = d
        return self

    def socket_linger(self, ms: int) -> Self:
        self._socket_linger = ms
        return self

    def zmq_job_addr(self, addr: str) -> Self:
        self._zmq_job_addr = addr
        return self

    def zmq_result_addr(self, addr: str) -> Self:
        self._zmq_result_addr = addr
        return self

    def min_insert_batch_logs(self, n: int) -> Self:
        self._min_insert_batch_logs = n
        return self

    def min_insert_batch_results(self, n: int) -> Self:
        self._min_insert_batch_results = n
        return self

    def flush_interval(self, s: float) -> Self:
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
            min_insert_batch_logs=self._min_insert_batch_logs,
            min_insert_batch_results=self._min_insert_batch_results,
            flush_interval=self._flush_interval,
        )


class AfterMap:
    """
    Class to store settings, and spawn processes.

    Use AfterMap.builder() to create an instance with custom settings.
    """

    @classmethod
    def builder(cls) -> AfterMapBuilder:
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
        socket_linger: int = 100,
        zmq_job_addr: str = "tcp://127.0.0.1:5554",
        zmq_result_addr: str = "tcp://127.0.0.1:5555",
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
            )
            worker.start()
            workers.append(worker)

        time.sleep(self.worker_startup_timeout)

        zctx = zmq.Context()
        job_sock = zctx.socket(zmq.PUSH)
        job_sock.bind(self.zmq_job_addr)

        for instance_id, inp in enumerate(inputs):
            cmd = LoadCommand(data=inp.model_dump(), instanceid=instance_id)
            job_sock.send(worker_adapter.dump_json(cmd))
            time.sleep(self.inter_job_delay)

        for _ in range(self.num_workers):
            cmd = DismissCommand()
            job_sock.send(worker_adapter.dump_json(cmd))

        for worker in workers:
            worker.join()

        job_sock.close(linger=self.socket_linger)

        writer_sock = zctx.socket(zmq.PUSH)
        writer_sock.connect(self.zmq_result_addr)
        writer_sock.send(writer_adapter.dump_json(TermCommand()))
        writer_sock.close(linger=self.socket_linger)
        writer_proc.join(timeout=5)

        zctx.term()
