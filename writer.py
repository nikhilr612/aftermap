"""
Module implementing the writer process responsible for logging and persisting worker results.
"""
from zmq.backend import Socket
import logging.config
import logging
import time
from sqlalchemy.orm import Session
from typing import Literal, Annotated
from pydantic import BaseModel, Field, TypeAdapter
import zmq
from schema import Base, LogSchema, ResultSchema, LogRecord, PersistedResult, LogType
import sqlalchemy

import multiprocessing


class LogCommand(BaseModel):
    cmd: Literal["log"] = "log"
    data: LogSchema


class ResultCommand(BaseModel):
    cmd: Literal["result"] = "result"
    data: ResultSchema


class TermCommand(BaseModel):
    cmd: Literal["term"] = "term"


type Command = Annotated[
    LogCommand | ResultCommand | TermCommand, Field(discriminator="cmd")
]

adapter: TypeAdapter[Command] = TypeAdapter(Command)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class Writer(multiprocessing.Process):
    """
    A process dedicated to ensure persistence of logs and computed results to an sqlite3 db.
    """    

    def __init__(
        self,
        dbpath: str,
        runid: int,
        zmq_bind_addr: str = "tcp://127.0.0.1:5555",
        min_insert_batch_logs: int = 32,
        min_insert_batch_results: int = 8,
        flush_interval: float = 60,
    ):
        self.dbpath = dbpath
        self.runid = runid
        self.zmq_bind_addr = zmq_bind_addr
        self.min_insert_batch_results = min_insert_batch_results
        self.min_insert_batch_logs = min_insert_batch_logs
        self.flush_interval = flush_interval

    def drain_queue(self, zsock: Socket[bytes], log_buffer: list[LogRecord], result_buffer: list[PersistedResult]) -> bool:
        """
          Drain the queue, appending records to the corresponding buffers for each command and return True if a Termination command was received.  
        """
        received_term = False
        try:
            while True:
                raw = zsock.recv()
                cmd = adapter.validate_json(raw)

                match cmd:
                    case TermCommand():
                        received_term = True
                        log_buffer.append(LogRecord(LogSchema(LogType.Finish, -1, self.runid)))
                        logger.warning("Writer termination requested.")
                    case LogCommand(data=logdata):
                        log_buffer.append(LogRecord(logdata))
                    case ResultCommand(data=resultdata):
                        result_buffer.append(PersistedResult(resultdata))
        except zmq.Again:
            pass

        return received_term

    def run(self):
        logger.info(f"Connecting to sqlite3 at {self.dbpath}")
        engine = sqlalchemy.create_engine(f"sqlite:///{self.dbpath}")
        Base.metadata.create_all(engine)

        session = Session(engine)
        zctx = zmq.Context()

        logger.info("Created session and ZMQ context.")
        
        zsock = zctx.socket(zmq.PULL)
        zsock.setsockopt(zmq.LINGER, 0) # no linger
        zsock.bind(self.zmq_bind_addr)
        
        logger.info(f"Bound to {self.zmq_bind_addr}")
    
        log_buffer: list[LogRecord] = [LogRecord(LogSchema(LogType.Start, -1, self.runid))]
        result_buffer: list[PersistedResult] = []
        last_flush = time.monotonic()
        
        while True:
            term_requested = self.drain_queue(zsock, log_buffer, result_buffer)
            must_flush = term_requested or (time.monotonic() - last_flush) > self.flush_interval
            log_buffer_sufficiently_full = len(log_buffer) > self.min_insert_batch_logs
            result_buffer_sufficiently_full = len(result_buffer) > self.min_insert_batch_results
            requires_commit = must_flush or log_buffer_sufficiently_full or result_buffer_sufficiently_full
            
            if must_flush or log_buffer_sufficiently_full:
                session.add_all(log_buffer)
                log_buffer.clear()
            if must_flush or result_buffer_sufficiently_full:
                session.add_all(result_buffer)
                result_buffer.clear()
            if requires_commit:
                session.commit()

            if term_requested:
                logger.info("Terminating gracefully...")
                break
        
        zsock.close()        
        zctx.term()
        session.close()
        logger.info("Writer exit.")
