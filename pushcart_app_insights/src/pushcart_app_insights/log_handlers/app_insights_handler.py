import logging

from azure.monitor.opentelemetry.exporter import AzureMonitorLogExporter
from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor


class ApplicationInsightsHandler(LoggingHandler):
    def __init__(
        self,
        connection_string: str,
        level: int = logging.NOTSET,
        logger_provider: LoggerProvider = None,
    ) -> None:
        logger_provider = logger_provider or LoggerProvider()
        exporter = AzureMonitorLogExporter(connection_string=connection_string)
        logger_provider.add_log_record_processor(
            BatchLogRecordProcessor(exporter=exporter)
        )
        super().__init__(level, logger_provider)
