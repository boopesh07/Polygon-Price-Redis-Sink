import logging
import sys
from typing import Any

try:
    import structlog
except ImportError:  # pragma: no cover - fallback when structlog missing (e.g., local tests)
    structlog = None  # type: ignore[assignment]


def configure_logging(debug: bool = False) -> None:
    level = logging.DEBUG if debug else logging.INFO
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=level,
    )

    if structlog is None:
        logging.getLogger(__name__).warning(
            "structlog_not_available", fallback_logger="logging", level=level
        )
        return

    timestamper = structlog.processors.TimeStamper(fmt="iso")
    structlog.configure(
        processors=[
            structlog.stdlib.filter_by_level,
            timestamper,
            structlog.stdlib.add_logger_name,
            structlog.stdlib.add_log_level,
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer(),
        ],
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )


def get_logger() -> Any:
    if structlog is not None:
        return structlog.get_logger()
    return logging.getLogger("polygon_sink")


