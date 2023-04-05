import logging
import threading
from datetime import datetime
from pathlib import Path
from queue import Full, Queue
from time import time
from typing import Optional

from delta.tables import DeltaTable
from pydantic import conint, dataclasses, validator
from pyspark.sql.types import _parse_datatype_string

from pushcart import spark


@dataclasses.dataclass
class DeltaTableHandler(logging.Handler):
    table_path: Path
    buffer_size: Optional[conint(strict=True, gt=0)] = 100
    flush_interval: Optional[conint(strict=True, gt=0)] = 15
    """
    The DeltaTableHandler class is a logging handler that writes log records to a Delta
    table in Apache Spark. It buffers log records and periodically flushes them to the
    Delta table. The class also initializes the Delta table if it does not exist.

    Fields:
    - table_path: the path to the Delta table.
    - buffer: a queue that stores log records.
    - flush_interval: the interval in seconds to flush the buffer.
    - timer: a threading.Timer object that periodically flushes the buffer.
    - schema: the schema of the Delta table.
    """

    @validator("table_path", pre=False, always=True)
    @classmethod
    def convert_to_absolute_string(cls, value: Optional[Path]) -> Optional[str]:
        """
        Validator that converts the Path object to its absolute POSIX representation
        """
        if value:
            return value.absolute().as_posix()

        return None

    def __post_init_post_parse__(self):
        """
        Initializes the DeltaTableHandler object with the table path, buffer size, and
        flush interval. It also initializes the buffer, schema, and starts the flush
        timer.
        """
        self.buffer = Queue(maxsize=self.buffer_size)
        self.timer = None
        self.schema = _parse_datatype_string(
            "struct<timestamp:timestamp,origin:string,level:string,message:string>"
        )
        self._init_table()
        self._start_flush_timer()

    def _init_table(self):
        """
        Checks if the Delta table exists and creates it if it does not. It also sets
        the DeltaTable object for the table.
        """
        if not DeltaTable.isDeltaTable(spark, self.table_path):
            spark.createDataFrame([], self.schema).write.format("delta").save(
                self.table_path
            )

        self.delta_table = DeltaTable.forPath(spark, self.table_path)

    def emit(self, record):
        """
        Adds a log record to the buffer and flushes the buffer if it is full or if the
        oldest record in the buffer is older than 60 seconds.
        """
        try:
            self.buffer.put(record)
        except Full:
            self.flush_buffer()
            self.buffer.put(record)

        if time() - self.buffer.queue[0].created > 60:
            self.flush_buffer()

    def flush_buffer(self):
        """
        Converts log records in the buffer to rows and writes them to the Delta table.
        """
        rows = []

        while not self.buffer.empty():
            record = self.buffer.get()
            log_entry = {
                "timestamp": datetime.fromtimestamp(record.created),
                "origin": record.name,
                "level": record.levelname,
                "message": record.getMessage(),
            }
            rows.append(log_entry)

        if rows:
            df = spark.createDataFrame(rows, schema=self.schema)
            df.write.format("delta").mode("append").save(self.table_path)

    def _start_flush_timer(self):
        """
        Starts a timer to periodically flush the buffer.
        """
        self.timer = threading.Timer(self.flush_interval, self._timer_flush)
        self.timer.start()

    def _timer_flush(self):
        """
        Cancels the current timer, flushes the buffer, and starts a new timer.
        """
        self.close()
        self._start_flush_timer()

    def close(self):
        """
        Cancels the current timer and flushes the buffer.
        """
        if self.timer:
            self.timer.cancel()
            self.timer = None

        self.flush_buffer()

    def __del__(self):
        self.close()
