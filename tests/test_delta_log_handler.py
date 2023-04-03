import logging
from datetime import datetime
from time import time

import pytest

from pushcart.log_handlers import DeltaTableHandler


@pytest.fixture(autouse=True)
def mock_spark_and_delta(mocker):
    """
    Mock DeltaTable and createDataFrame methods
    """
    delta_table = mocker.patch("pushcart.log_handlers.delta_table_handler.DeltaTable")
    spark = mocker.patch("pushcart.log_handlers.delta_table_handler.spark")

    yield delta_table, spark


class TestDeltaTableHandler:
    def test_init_table(self, request, mock_spark_and_delta):
        """
        Tests that the delta table is correctly initialized when it does not exist.
        """
        delta_table, spark = mock_spark_and_delta
        delta_table.isDeltaTable.return_value = False

        handler = DeltaTableHandler("/delta/table/path")
        request.addfinalizer(handler.close)

        # Assert that DeltaTable and createDataFrame were called with the correct arguments
        delta_table.isDeltaTable.assert_called_once_with(spark, "/delta/table/path")
        delta_table.forPath.assert_called_once_with(spark, "/delta/table/path")
        spark.createDataFrame.assert_called_once()

    def test_flush_buffer_full(self, mocker, request, mock_spark_and_delta):
        """
        Tests that the buffer is correctly flushed when it is full.
        """
        delta_table, spark = mock_spark_and_delta
        delta_table.isDeltaTable.return_value = True

        # Set buffer size to 2 for easier testing
        handler = DeltaTableHandler("/delta/table/path", buffer_size=2)
        request.addfinalizer(handler.close)

        # Add two records to the buffer to fill it up
        record1 = logging.LogRecord(
            "test", logging.INFO, None, None, "Test message", None, None
        )
        record2 = logging.LogRecord(
            "test", logging.INFO, None, None, "Test message 2", None, None
        )
        handler.buffer.put(record1)
        handler.buffer.put(record2)

        # Call flush_buffer method
        handler.flush_buffer()

        # Assert that DeltaTable and createDataFrame were called with the correct arguments
        delta_table.forPath.assert_called_once_with(spark, "/delta/table/path")
        spark.createDataFrame.assert_called_with(
            [
                {
                    "timestamp": mocker.ANY,
                    "origin": "test",
                    "level": "INFO",
                    "message": "Test message",
                },
                {
                    "timestamp": mocker.ANY,
                    "origin": "test",
                    "level": "INFO",
                    "message": "Test message 2",
                },
            ],
            schema=handler.schema,
        )

    def test_timer_cancel(self, mocker, request, mock_spark_and_delta):
        """
        Tests that the timer is correctly canceled when the handler is closed.
        """
        delta_table, _ = mock_spark_and_delta
        delta_table.isDeltaTable.return_value = True

        mock_timer = mocker.patch("threading.Timer")
        mock_flush_buffer = mocker.patch(
            "pushcart.log_handlers.delta_table_handler.DeltaTableHandler.flush_buffer"
        )

        handler = DeltaTableHandler("/delta/table/path")
        request.addfinalizer(handler.close)
        handler.close()

        # Assert that Timer was canceled and flush_buffer was called
        mock_timer.assert_called_once_with(handler.flush_interval, handler._timer_flush)
        mock_timer.return_value.cancel.assert_called_once()
        mock_flush_buffer.assert_called_once()

    def test_flush_buffer_empty(self, request, mock_spark_and_delta):
        """
        Tests that the buffer is not flushed when it is empty.
        """
        delta_table, spark = mock_spark_and_delta
        delta_table.isDeltaTable.return_value = True

        handler = DeltaTableHandler(
            "/delta/table/path", buffer_size=100, flush_interval=15
        )
        request.addfinalizer(handler.close)

        handler.flush_buffer()

        spark.createDataFrame.assert_not_called()

    def test_emit_older_than_60(self, mocker, request, mock_spark_and_delta):
        """
        Tests that the buffer is correctly flushed when the oldest record is older than 60 seconds.
        """
        delta_table, spark = mock_spark_and_delta
        delta_table.isDeltaTable.return_value = True

        handler = DeltaTableHandler(
            "/delta/table/path", buffer_size=100, flush_interval=15
        )
        request.addfinalizer(handler.close)

        record = logging.LogRecord(
            "test", logging.INFO, None, None, "Test message", None, None
        )
        record.created = time() - 61
        handler.emit(record)

        spark.createDataFrame.assert_called_with(
            [
                {
                    "timestamp": mocker.ANY,
                    "origin": "test",
                    "level": "INFO",
                    "message": "Test message",
                }
            ],
            schema=handler.schema,
        )

    def test_correctness_of_records(self, mocker, request, mock_spark_and_delta):
        """
        Tests that the log records written to the delta table are correct and match the
        expected schema.
        """
        delta_table, spark = mock_spark_and_delta
        delta_table.isDeltaTable.return_value = True

        df = spark.createDataFrame.return_value

        df_write = mocker.Mock()
        type(df).write = mocker.PropertyMock(return_value=df_write)

        df_write_format = mocker.Mock()
        df_write.return_value = df_write_format

        df_write_mode = mocker.Mock()
        df_write_format.return_value = df_write_mode

        df_write_save = mocker.Mock()
        df_write_mode.return_value = df_write_save

        handler = DeltaTableHandler(
            "/delta/table/path", buffer_size=100, flush_interval=15
        )
        request.addfinalizer(handler.close)

        record = logging.LogRecord(
            "test", logging.INFO, None, None, "Test message", None, None
        )
        record.created = time()
        handler.emit(record)

        handler.flush_buffer()

        spark.createDataFrame.assert_called_once_with(
            [
                {
                    "timestamp": datetime.fromtimestamp(record.created),
                    "origin": "test",
                    "level": "INFO",
                    "message": "Test message",
                }
            ],
            schema=handler.schema,
        )
