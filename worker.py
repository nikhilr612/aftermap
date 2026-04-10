"""
Module to implement worker processes running jobs from a shared queue.
"""

from datetime import datetime

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

    def info(self, line: str, tag: str = ""):
        """
        Log information to persistent storage.
        """
        self._zsock.send(
            writer.adapter.dump_json(
                writer.LogCommand(
                    data=LogSchema(
                        LogType.Info, self._instanceid, self._runid, line, tag
                    )
                )
            )
        )

    def debug(self, line: str, tag: str = ""):
        """
        Log debug information to persistent storage.
        """
        self._zsock.send(
            writer.adapter.dump_json(
                writer.LogCommand(
                    data=LogSchema(
                        LogType.Debug, self._instanceid, self._runid, line, tag
                    )
                )
            )
        )


class LoadCommand(BaseModel, Generic[TInput]):
    cmd: Literal["load"] = "load"
    data: TInput
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
        zmq_pull_addr: str = "tcp://127.0.0.1:5554",
        zmq_push_addr: str = "tcp://127.0.0.1:5555",
    ):
        self.worker_id = id
        self.input_adapter = TypeAdapter(input_type)
        self.output_adapter = TypeAdapter(output_type)
        self.fn: WorkerFunction = fn
        self.zmq_pull_addr = zmq_pull_addr
        self.zmq_push_addr = zmq_push_addr
        self.logger = logging.getLogger(f"{__name__}${id}")
        self.logger.setLevel(logging.INFO)
        self.runid = runid

    def run(self):
        zctx = zmq.Context()
        zctx.setsockopt(zmq.LINGER, 0)
        zsock = zctx.socket(zmq.PUSH)
        zsock.connect(self.zmq_push_addr)

        jsock = zctx.socket(zmq.PULL)
        jsock.connect(self.zmq_pull_addr)

        while True:
            raw = jsock.recv()
            cmd = adapter.validate_json(raw)
            match cmd:
                case DismissCommand():
                    break
                case LoadCommand(data=jobdata, instanceid=instanceid):
                    ctx = RunnerContext(instanceid, self.runid, zsock)
                    out: TOutput | None = self.fn(ctx, jobdata)
                    ts = datetime.now()

                    zsock.send(
                        writer.adapter.dump_json(
                            writer.LogCommand(
                                data=LogSchema(
                                    LogType.Failure if out is None else LogType.Success,
                                    instanceid,
                                    self.runid,
                                    ctx._get_comment_line(),
                                )
                            )
                        )
                    )

                    if out is not None:
                        json_data = self.output_adapter.dump_json(out)
                        zsock.send(
                            writer.adapter.dump_json(
                                writer.ResultCommand(
                                    data=ResultSchema(
                                        ts, instanceid, self.runid, json_data
                                    )
                                )
                            )
                        )


        zsock.close()
        jsock.close()
        zctx.term()
