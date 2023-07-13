"""Custom logging handler that writes log records to a Delta table in Apache Spark.

This handler buffers log records and periodically flushes them to the Delta table. If the Delta table does not exist, it initializes it.

Parameters
----------
table_path : Path
    The path to the Delta table.
buffer_size : conint(strict=True, gt=0) | None, optional
    The maximum size of the buffer that stores log records, by default 100.
flush_interval : conint(strict=True, gt=0) | None, optional
    The interval in seconds to flush the buffer, by default 15.

Attributes
----------
buffer : Queue
    A queue that stores log records.
timer : threading.Timer
    A timer object that periodically flushes the buffer.
schema : pyspark.sql.types.StructType
    The schema of the Delta table.
delta_table : delta.tables.DeltaTable
    The Delta table object.

Methods
-------
emit(record)
    Emit a log record. Add a log record to the buffer and flush the buffer if it is full or if the oldest record in the buffer is older than 60 seconds.
flush_buffer()
    Convert log records in the buffer to rows and write them to the Delta table.
close()
    Cancel the current timer and flush the buffer.

Notes
-----
The `__post_init_post_parse__` method is called after the object is initialized. It initializes the buffer, schema, and starts the flush timer.
The `_init_table` method checks if the Delta table exists and creates it if not. It also sets the DeltaTable object for the table.
The `_start_flush_timer` method starts a timer to periodically flush the buffer.
The `_timer_flush` method cancels the current timer, flushes the buffer, and starts a new timer.
The `__del__` method cancels the timer and flushes the buffer when the object is destroyed.

"""


import logging
import threading
from datetime import UTC, datetime
from pathlib import Path
from queue import Full, Queue
from time import time

from delta.tables import DeltaTable
from pydantic import conint, dataclasses, validator
from pyspark.sql.types import _parse_datatype_string

from pushcart import spark

FLUSH_BUFFER_TIMEOUT = 60


@dataclasses.dataclass
class DeltaTableHandler(logging.Handler):
    """Logging handler that writes log records to a Delta table in Apache Spark.

    Buffers log records and periodically flushes them to the Delta table. Also
    initializes the Delta table if it does not exist.

    Fields:
    - table_path: the path to the Delta table.
    - buffer: a queue that stores log records.
    - flush_interval: the interval in seconds to flush the buffer.
    - timer: a threading.Timer object that periodically flushes the buffer.
    - schema: the schema of the Delta table.
    """

    table_path: Path
    buffer_size: conint(strict=True, gt=0) | None = 100
    flush_interval: conint(strict=True, gt=0) | None = 15

    @validator("table_path", pre=False, always=True)
    @classmethod
    def convert_to_absolute_string(cls, value: Path | None) -> str | None:
        """Convert the Path object to its absolute POSIX representation."""
        if value:
            return value.absolute().as_posix()

        return None

    def __post_init_post_parse__(self) -> None:
        """Initialize the DeltaTableHandler.

        Initialized object with the table path, buffer size, and flush interval. Also
        initialize the buffer, schema, and start the flush timer.
        """
        self.buffer = Queue(maxsize=self.buffer_size)
        self.timer = None
        self.schema = _parse_datatype_string(
            "struct<timestamp:timestamp,origin:string,level:string,message:string>",
        )
        self._init_table()
        self._start_flush_timer()

    def _init_table(self) -> None:
        """Check if the Delta table exists and creates it if not.

        Also sets the DeltaTable object for the table.
        """
        if not DeltaTable.isDeltaTable(spark, self.table_path):
            spark.createDataFrame([], self.schema).write.format("delta").save(
                self.table_path,
            )

        self.delta_table = DeltaTable.forPath(spark, self.table_path)

    def emit(self, record: logging.LogRecord) -> None:
        """Emit a log record.

        Add a log record to the buffer and flush the buffer if it is full or if the
        oldest record in the buffer is older than 60 seconds.
        """
        try:
            self.buffer.put(record)
        except Full:
            self.flush_buffer()
            self.buffer.put(record)

        if time() - self.buffer.queue[0].created > FLUSH_BUFFER_TIMEOUT:
            self.flush_buffer()

    def flush_buffer(self) -> None:
        """Convert log records in the buffer to rows and write them to the Delta table."""
        rows = []

        while not self.buffer.empty():
            record = self.buffer.get()
            log_entry = {
                "timestamp": datetime.fromtimestamp(record.created, tz=UTC),
                "origin": record.name,
                "level": record.levelname,
                "message": record.getMessage(),
            }
            rows.append(log_entry)

        if rows:
            log_df = spark.createDataFrame(rows, schema=self.schema)
            log_df.write.format("delta").mode("append").save(self.table_path)

    def _start_flush_timer(self) -> None:
        """Start a timer to periodically flush the buffer."""
        self.timer = threading.Timer(self.flush_interval, self._timer_flush)
        self.timer.start()

    def _timer_flush(self) -> None:
        """Cancel the current timer, flush the buffer, and start a new timer."""
        self.close()
        self._start_flush_timer()

    def close(self) -> None:
        """Cancel the current timer and flush the buffer."""
        if self.timer:
            self.timer.cancel()
            self.timer = None

        self.flush_buffer()

    def __del__(self) -> None:
        """Cancel the timer and flush the buffer on object destruction."""
        self.close()
