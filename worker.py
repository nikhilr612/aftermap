"""
Module to implement worker processes running jobs from a shared queue.
"""

from datetime import datetime
import traceback

import zmq
import logging
import writer
from schema import LogSchema, LogType, ResultSchema
from zmq import Socket

import multiprocessing
from pydantic import BaseModel, TypeAdapter, Field
from typing import TypeVar, Generic, Callable, Literal, Annotated

TInput = TypeVar("TInput", bound=BaseModel)
TOutput = TypeVar("TOutput", bound=BaseModel)


class RunnerContext:
    """
    Context passed to worker functions for logging and tracking.
    Provides access to instance ID, run ID, and logging methods.
    """

    _instanceid: int
    _runid: int
    _zsock: Socket[bytes]
    _comments: list[str] = []

    def __init__(self, instanceid: int, runid: int, zsock: Socket[bytes]):
        self._instanceid = instanceid
        self._runid = runid
        self._zsock = zsock

    @property
    def instance(self) -> int:
        """
        The ID for the current job or instance
        """
        return self._instanceid

    @property
    def run(self) -> int:
        """
        The ID for the current run.
        """
        return self._runid

    def comment(self, line: str) -> None:
        """
        Add a line of comment for this task.
        All comment lines are concatenated to create the comment line for the final SUCCESS or FAILURE log after execution.
        """
        self._comments.append(line)

    def _get_comment_line(self) -> str:
        return "\n".join(self._comments)

    def info(self, line: str, tag: str = "") -> None:
        """
        Log an info message for this job instance.

        :param line: The log message
        :param tag: Optional tag for filtering logs
        """
        self._zsock.send(
            writer.adapter.dump_json(
                writer.LogCommand(
                    data=LogSchema.new(
                        LogType.Info, self._instanceid, self._runid, line, tag
                    )
                )
            )
        )

    def debug(self, line: str, tag: str = "") -> None:
        """
        Log a debug message for this job instance.

        :param line: The log message
        :param tag: Optional tag for filtering logs
        """
        self._zsock.send(
            writer.adapter.dump_json(
                writer.LogCommand(
                    data=LogSchema.new(
                        LogType.Debug, self._instanceid, self._runid, line, tag
                    )
                )
            )
        )


class LoadCommand(BaseModel, Generic[TInput]):
    cmd: Literal["load"] = "load"
    data: dict
    instanceid: int


class DismissCommand(BaseModel):
    cmd: Literal["kys"] = "kys"


type WorkerCommand = Annotated[LoadCommand | DismissCommand, Field(discriminator="cmd")]

adapter: TypeAdapter[WorkerCommand] = TypeAdapter(WorkerCommand)

type WorkerFunction[TInput, TOutput] = Callable[[RunnerContext, TInput], TOutput | None]


class PullWorker(multiprocessing.Process, Generic[TInput, TOutput]):
    def __init__(
        self,
        id: int,
        runid: int,
        input_type: type[TInput],
        output_type: type[TOutput],
        fn: WorkerFunction[TInput, TOutput],
        zmq_job_addr: str = "tcp://127.0.0.1:5554",
        zmq_result_addr: str = "tcp://127.0.0.1:5555",
    ):
        multiprocessing.Process.__init__(self)
        self.worker_id = id
        self.input_adapter = TypeAdapter(input_type)
        self.output_adapter = TypeAdapter(output_type)
        self.fn: WorkerFunction = fn
        self.zmq_job_addr = zmq_job_addr
        self.zmq_result_addr = zmq_result_addr
        self.logger = logging.getLogger(f"{__name__}${id}")
        self.logger.setLevel(logging.INFO)
        self.runid = runid

    def run(self):
        zctx = zmq.Context()
        zctx.setsockopt(zmq.LINGER, 0)
        zsock = zctx.socket(zmq.PUSH)
        zsock.connect(self.zmq_result_addr)

        jsock = zctx.socket(zmq.PULL)
        jsock.connect(self.zmq_job_addr)

        while True:
            raw = jsock.recv()
            cmd = adapter.validate_json(raw)
            match cmd:
                case DismissCommand():
                    break
                case LoadCommand(data=jobdata, instanceid=instanceid):
                    self.process_job(jobdata, instanceid, zsock)

        zsock.close()
        jsock.close()
        zctx.term()

    def process_job(self, jobdata, instanceid, zsock):
        ctx = RunnerContext(instanceid, self.runid, zsock)
        typed_data = self.input_adapter.validate_python(jobdata)
        try:
            out: TOutput | None = self.fn(ctx, typed_data)
        except Exception:
            zsock.send(
                writer.adapter.dump_json(
                    writer.LogCommand(
                        data=LogSchema.new(
                            LogType.Abort,
                            instanceid,
                            self.runid,
                            traceback.format_exc(),
                        )
                    )
                )
            )
            return

        ts = datetime.now()
        zsock.send(
            writer.adapter.dump_json(
                writer.LogCommand(
                    data=LogSchema.new(
                        LogType.Failure if out is None else LogType.Success,
                        instanceid,
                        self.runid,
                        ctx._get_comment_line(),
                    )
                )
            )
        )

        if out is not None:
            json_data = self.output_adapter.dump_json(out).decode()  # get string
            zsock.send(
                writer.adapter.dump_json(
                    writer.ResultCommand(
                        data=ResultSchema.new(ts, instanceid, self.runid, json_data)
                    )
                )
            )
