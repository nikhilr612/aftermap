"""
Module to document all database/persistence schemas 
"""
from pydantic import BaseModel
from typing import Any

from sqlalchemy import LargeBinary, func
from sqlalchemy.orm import Mapped, DeclarativeBase, mapped_column
from datetime import datetime

import enum
import json

# base class for sql alchemy to aggregate schema
class Base(DeclarativeBase):
    pass

class LogType(enum.Enum):
    Success = 0 # automatically inserted when an instance completes
    Failure = 1 # automatically inserted when an instance fails
    Abort = 2 # automatically inserted when an instance errors out or ends abruptly
    Info = 3 
    Debug = 4
    Start = 5 # automatically inserted whenever a run starts
    Finish = 6 # automatically inserted after a run ends

class LogSchema(BaseModel):
    """
        Schema for a log line.
    """
    instanceid: int
    runid: int
    logtype: LogType
    tag: str
    comment: str | None
    timestamp: datetime
    
    def __init__(self, logtype: LogType, instanceid: int, runid: int, comment: str | None = None, tag: str = ""):
        """
           Create a new log object with the specified log type, optional comment and tag.
           The timestamp associated with this log line is the current date and time.

           :param LogType logtype: The logtype
           :param str comment: An optional comment to be associated with the logline
           :param str tag: A tag to be associated with the log line, useful for filtering
        """
        self.timestamp = datetime.now() # use current time
        self.comment = comment
        self.logtype = logtype
        self.instanceid = instanceid
        self.runid = runid
        self.tag = tag.strip().replace(" ", "-")

        
class LogRecord(Base):
    """
        A log record
    """
    
    __tablename__ = "logs"
    
    logid: Mapped[int] = mapped_column(primary_key=True)
    instanceid: Mapped[int]
    runid: Mapped[int]
    logtype: Mapped[LogType]
    tag: Mapped[str]
    comment: Mapped[str | None]
    timestamp: Mapped[datetime]

    def __init__(self, log: LogSchema):
        """
           Create a record from an object conforming to the schema. 
        """
        self.timestamp = log.timestamp
        self.instanceid = log.instanceid
        self.runid = log.runid
        self.logtype = log.logtype
        self.comment = log.comment
        self.tag = log.tag

class ResultSchema(BaseModel):
    """
        Schema for a result obtained.
        Object data is stored as JSON.
    """
    
    timestamp: datetime
    instanceid: int
    runid: int
    json_data: str

    def __init__(self, timestamp: datetime, instanceid: int, runid: int, json_data: Any):
        """
           Create a new result object for instance completed at specified timestamp.
           The result data object must be serializable to JSON.
           The result data is persisted in the database as a Blob containing JSONB.
        """
        self.json_data = json_data
        self.instanceid = instanceid
        self.runid = runid
        self.timestamp = timestamp

class PersistedResult(Base):
    """
       A result record. 
    """
    __tablename__ = "results"
    
    timestamp: Mapped[datetime]
    instanceid: Mapped[int]
    runid: Mapped[int]
    blob: LargeBinary

    def __init__(self, result: ResultSchema):
        """
           Create a new result record from an object conforming to the schema.
           The JSON data is persisted in the database as a Blob containing JSONB.
        """
        self.blob = func.jsonb(result.json_data, type_=LargeBinary())
        self.timestamp = result.timestamp
        self.instanceid = result.instanceid
        self.runid = result.instanceid
