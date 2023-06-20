import logging
import sys


def get_logger(name="Ruia"):
    """
    Get logger
    Args:
        name (str, optional): logger name. Defaults to "Ruia".

    Returns:
        _type_: Logger
    """
    logging_format = f"[%(asctime)s] %(levelname)-5s %(name)-{len(name)}s "
    # logging_format += "%(module)-7s::l%(lineno)d: "
    # logging_format += "%(module)-7s: "
    logging_format += "%(message)s"

    logging.basicConfig(
        format=logging_format,
        level=logging.DEBUG,
        datefmt="%Y:%m:%d %H:%M:%S",
        stream=sys.stderr
    )
    logging.getLogger("asyncio").setLevel(logging.DEBUG)
    return logging.getLogger(name)