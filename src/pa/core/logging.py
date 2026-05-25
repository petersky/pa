import logging
import sys

from pa.config import Settings


def configure_logging(settings: Settings) -> None:
    level = logging.DEBUG if settings.debug else getattr(
        logging, settings.log_level.upper(), logging.INFO
    )
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)-8s [%(name)s] %(message)s",
        stream=sys.stderr,
        force=True,
    )
    if settings.debug:
        logging.getLogger("pa").setLevel(logging.DEBUG)
