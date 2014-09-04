import ConfigParser
import logging
import os
import subprocess

import psycopg2

from .database import database as db


# Set up logging system.
logging.basicConfig()
logger = logging.getLogger(__name__)


def load_config(config_dir='../config'):
    """Returns a ConfigParser object.

    Configuration is loaded from defaults.cfg and user.cfg in config_dir.

    """
    # Load configuration using ConfigParser.
    logger.debug("Loading configuration from %s" % config_dir)
    config = ConfigParser.RawConfigParser()
    try:
        config.read([os.path.join(config_dir, 'defaults.cfg'),
                     os.path.join(config_dir, 'user.cfg')])
        return config
    except ConfigParser.ParsingError:
        logger.exception("Error parsing configuration")
        raise


def logf(level, f):
    """Log each line of a file-like object at the specified severity level."""
    for line in f:
        if line:
            logger.log(level, line.strip())


class DataLoader(object):
    """Data loader class with support for importing shapefiles.

    Some example usage:

        loader = DataLoader()

        # Load data.
        loader.load_shp('parcel/Alameda.shp', 'staging.alameda')

        # Run SQL command and commit.
        with loader.database.cursor() as cur:
            cur.execute("SELECT DISTINCT luc_desc FROM staging.alameda;")
            rows = cur.fetchall()

        # Close all connection(s).
        loader.close()

    Methods:
        close:      Close managed PostgreSQL connection(s).
        load_shp:   Load a shapefile from the directory into a PostGIS table.

    Attributes:
        database:   PostgreSQL database connection manager class.
        directory:  Path to the directory containing the shapefiles.
        srid:       Default Spatial Reference System Identifier (SRID).

    Constructor arguments:
        config_dir: Path to configuration directory containing default.cfg
                    and/or user.cfg. If None, attributes must be passed as
                    additional constructor arguments. Otherwise, passed
                    attributes override configuration.

    """

    def __init__(self, config_dir='../config', database=None, directory=None,
                 srid=None):
        # If configuration directory is defined, load its configuration.
        if config_dir:
            config = load_config(config_dir)
            db_config = dict(config.items('database'))

        # Define attributes from configuration, unless overridden.
        if not database:
            database = db
        if not directory:
            directory = config.get('data', 'directory')
        if not srid:
            srid = config.get('data', 'srid')

        # Create new connection(s) using configuration, unless already
        # connected.
        try:
            database.assert_connected()
        except psycopg2.DatabaseError:
            database.connect(**db_config)

        # Assign arguments to class attributes.
        self.database = database
        if os.path.exists(directory):
            self.directory = directory
        else:
            raise IOError("Directory does not exist: %s" % directory)
        self.srid = int(srid)

    def close(self):
        return self.database.close()

    def load_shp(self, filename, table, srid=None, drop=False, append=False):
        """Load a shapefile from the directory into a PostGIS table.

        This is a Python wrapper for shp2gpsql. shp2pgsql is spawned by
        subprocess. Commands generated by shp2pgsql are executed on a
        psycopg2 cursor object. For performance, the PostgreSQL "dump"
        format is used instead of the default "insert" SQL format.

        Args:
            filename: Shapefile, relative to the data directory.
            table:    PostGIS table name (optionally schema-qualified).
            srid:     Spatial Reference System Identifier (SRID), if different
                      from data default.
            drop:     Whether to drop a table that already exists.
                      Defaults to False.
            append:   Whether to append to an existing table, instead of
                      creating one. Defaults to False.
        """
        logger.info("Loading table %s from file %s." % (table, filename))
        filepath = os.path.join(self.directory, filename)

        # Use default SRID if not defined.
        if not srid:
            srid = self.srid

        with self.database.cursor() as cur:

            if drop:
                # Drop the existing table.
                cur.execute('DROP TABLE IF EXISTS %s' % table)

            if not append:
                # Create the new table itself without adding actual data.
                create_table = subprocess.Popen(['shp2pgsql', '-p', '-I',
                                                 '-s', str(srid),
                                                 filepath, table],
                                                stdout=subprocess.PIPE,
                                                stderr=subprocess.PIPE)
                try:
                    command = ''
                    for line in create_table.stdout:
                        if line and not (line.startswith('BEGIN') or
                                         line.startswith('COMMIT')):
                            command += line
                    cur.execute(command)
                finally:
                    logf(logging.DEBUG, create_table.stderr)
                create_table.wait()

            # Append data to existing or newly-created table.
            append_data = subprocess.Popen(['shp2pgsql', '-a', '-D', '-I',
                                            '-s', str(srid),
                                            filepath, table],
                                           stdout=subprocess.PIPE,
                                           stderr=subprocess.PIPE)
            try:
                while True:
                    line = append_data.stdout.readline()
                    if line.startswith('COPY'):
                        break
                cur.copy_expert(line, append_data.stdout)
            finally:
                logf(logging.DEBUG, append_data.stderr)
            append_data.wait()


def load_multiple_shp(shapefiles, config_dir):
    """
    Load multiple shapefiles to PostGIS according to a given dictionary
    of shapefile information.

    Parameters
    ----------
    shapefiles : dict
        Dictionary of dictionaries where the top-level key is shapefile category,
        which also corresponds to the name of the directory within the data_dir
        containing this category of shapefiles. The sub-dictionaries are
        dictionaries where the keys correspond to database table name and the
        value is a tuple of the form (shapefile_file_name, SRID).  If SRID is
        None, then default config SRID is used.

        Example dictionary
             {'parcels' :  ##Looks for 'parcels' directory within the data_dir
                  ##Looks for 'marin' directory within parcels dir
                  {'marin':('Marin_2006_CWP.shp', 2872),
                  'napa':('Napa_Parcels.shp', 2226),
                  },
              'boundaries' :
                  {'blocks':('block10_gba.shp', 26910),
                   'block_groups':('blockgroup10_gba.shp',26910),
                  },
             }
    config_dir : str
        Path to spandex configuration directory. Configuration should specify
        the input data directory (data_dir).  The data_dir should contain
        subdirectories corresponding to each shapefile category, which in turn
        should contain a subdirectory for each shapefile.

    Returns
    -------
    None : None
        Loads shapefiles to the database (returns nothing)

    """
    def subpath(base_dir):
        def func(shp_table_name, shp_path):
            input_dir = base_dir
            return os.path.join(input_dir, shp_table_name, shp_path)
        return func

    loader = DataLoader(config_dir=config_dir)

    for shape_category in shapefiles:
        path_func = subpath(shape_category)
        shp_dict = shapefiles[shape_category]
        for shp_name in shp_dict:
            print 'Loading %s.' % shp_name
            path = path_func(shp_name, shp_dict[shp_name][0])
            loader.load_shp(filename=path,
                table=shape_category + '_' + shp_name,
                srid=shp_dict[shp_name][1], drop=True)
