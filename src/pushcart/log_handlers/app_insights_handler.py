"""Log handler for Application Insights."""

import logging

from azure.monitor.opentelemetry.exporter import (  # type: ignore[import-not-found]
    AzureMonitorLogExporter,
)
from opentelemetry.sdk._logs import (  # type: ignore[import-not-found]
    LoggerProvider,
    LoggingHandler,
)
from opentelemetry.sdk._logs.export import (  # type: ignore[import-not-found]
    BatchLogRecordProcessor,
)


class ApplicationInsightsHandler(LoggingHandler):
    """Log handler class for Application Insights.

    Args:
    ----
        LoggingHandler (_type_): Python SDK LoggingHandler class

    """

    def __init__(
        self,
        connection_string: str,
        level: int = logging.NOTSET,
        logger_provider: LoggerProvider | None = None,
    ) -> None:
        logger_provider = logger_provider or LoggerProvider()
        exporter = AzureMonitorLogExporter(connection_string=connection_string)
        logger_provider.add_log_record_processor(
            BatchLogRecordProcessor(exporter=exporter),
        )
        super().__init__(level, logger_provider)
