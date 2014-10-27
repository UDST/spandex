import logging
import os

from six.moves import configparser


"""Contains reusable utility functions."""


# Set up logging system.
logging.basicConfig()
logger = logging.getLogger(__name__)


def load_config(config_filename=None):
    """Returns a configparser object.

    Configuration is loaded from these filenames, in increasing precedence:

      - ~/.spandex/user.cfg
      - SPANDEX_CFG environment variable
      - config_filename argument, if provided

    If a file cannot be opened, it will be ignored. If none of the filenames
    can be opened, the configparser object will be empty.

    """
    # Build list of configuration filenames.
    config_filenames = [os.path.expanduser('~/.spandex/user.cfg')]
    config_filename_env = os.environ.get('SPANDEX_CFG')
    if config_filename_env:
        config_filenames.append(config_filename_env)
    if config_filename:
        config_filenames.append(config_filename)

    # Load configuration using configparser.
    logger.debug("Loading configuration from %s" % config_filenames)
    config = configparser.RawConfigParser()
    config.read(config_filenames)
    return config


def logf(level, f):
    """Log each line of a file-like object at the specified severity level."""
    for line in f:
        line = line.strip()
        if line:
            if (line.startswith(b"Shapefile type: ") or
                    line.startswith(b"Postgis type: ")):
                # Send usual shp2pgsql stderr messages to debug log.
                logger.debug(line)
            else:
                # Otherwise, stderr message may be important.
                logger.log(level, line)
