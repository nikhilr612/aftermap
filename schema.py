"""
Module to document all database/persistence schemas
"""

from pydantic import BaseModel
from typing import Self

from sqlalchemy import LargeBinary, func
from sqlalchemy.orm import Mapped, DeclarativeBase, mapped_column
from datetime import datetime

import enum


# base class for sql alchemy to aggregate schema
class Base(DeclarativeBase):
    pass


class LogType(enum.Enum):
    Success = 0  # automatically inserted when an instance completes
    Failure = 1  # automatically inserted when an instance fails
    Abort = 2  # automatically inserted when an instance errors out or ends abruptly
    Info = 3
    Debug = 4
    Start = 5  # automatically inserted whenever a run starts
    Finish = 6  # automatically inserted after a run ends


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

    @classmethod
    def new(
        cls,
        logtype: LogType,
        instanceid: int,
        runid: int,
        comment: str | None = None,
        tag: str = "",
    ) -> Self:
        """
        Create a new log object with the specified log type, optional comment and tag.
        The timestamp associated with this log line is the current date and time.

        :param LogType logtype: The logtype
        :param str comment: An optional comment to be associated with the logline
        :param str tag: A tag to be associated with the log line, useful for filtering
        """
        timestamp = datetime.now()  # use current time
        tag = tag.strip().replace(" ", "-")
        return cls(
            instanceid=instanceid,
            runid=runid,
            logtype=logtype,
            comment=comment,
            tag=tag,
            timestamp=timestamp,
        )

    def into_record(self) -> LogRecord:
        """
        Convert this schema into an actual record
        """
        return LogRecord(
            instanceid=self.instanceid,
            runid=self.runid,
            logtype=self.logtype,
            comment=self.comment,
            tag=self.tag,
            timestamp=self.timestamp,
        )


class PersistedResult(Base):
    """
    A result record.
    """

    __tablename__ = "results"

    resultid: Mapped[int] = mapped_column(primary_key=True)
    timestamp: Mapped[datetime]
    instanceid: Mapped[int]
    runid: Mapped[int]
    blob: Mapped[bytes] = mapped_column(LargeBinary)


class ResultSchema(BaseModel):
    """
    Schema for a result obtained.
    Object data is stored as JSON.
    """

    timestamp: datetime
    instanceid: int
    runid: int
    json_data: str

    @classmethod
    def new(
        cls, timestamp: datetime, instanceid: int, runid: int, json_data: str
    ) -> Self:
        """
        Create a new result object for instance completed at specified timestamp.
        The result data object must be serializable to JSON.
        The result data is persisted in the database as a Blob containing JSONB.
        """
        return cls(
            timestamp=timestamp, instanceid=instanceid, runid=runid, json_data=json_data
        )

    def into_record(self) -> PersistedResult:
        """
        Create a new result record from this object.
        The JSON data is persisted in the database as a Blob containing JSONB.
        """
        blob = func.jsonb(self.json_data, type_=LargeBinary())
        timestamp = self.timestamp
        instanceid = self.instanceid
        runid = self.runid
        return PersistedResult(
            timestamp=timestamp, instanceid=instanceid, runid=runid, blob=blob
        )
