import ConfigParser
import json
import logging
import os
import subprocess
from urllib import urlencode
from urllib2 import urlopen

from osgeo import osr
import psycopg2

from .database import database as db


# Set up logging system.
logging.basicConfig()
logger = logging.getLogger(__name__)


def load_config(config_filename=None):
    """Returns a ConfigParser object.

    Configuration is loaded from these filenames, in increasing precedence:

      - ~/.spandex/user.cfg
      - SPANDEX_CFG environment variable
      - config_filename argument, if provided

    If a file cannot be opened, it will be ignored. If none of the filenames
    can be opened, the ConfigParser object will be empty.

    """
    # Build list of configuration filenames.
    config_filenames = [os.path.expanduser('~/.spandex/user.cfg')]
    config_filename_env = os.environ.get('SPANDEX_CFG')
    if config_filename_env:
        config_filenames.append(config_filename_env)
    if config_filename:
        config_filenames.append(config_filename)

    # Load configuration using ConfigParser.
    logger.debug("Loading configuration from %s" % config_filenames)
    config = ConfigParser.RawConfigParser()
    config.read(config_filenames)
    return config


def logf(level, f):
    """Log each line of a file-like object at the specified severity level."""
    for line in f:
        line = line.strip()
        if line:
            if (line.startswith("Shapefile type: ") or
                line.startswith("Postgis type: ")):
                # Send usual shp2pgsql stderr messages to debug log.
                logger.debug(line)
            else:
                # Otherwise, stderr message may be important.
                logger.log(level, line)


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
        close:           Close managed PostgreSQL connection(s).
        get_encoding:    Identify shapefile attribute encoding.
        get_srid:        Identify shapefile EPSG SRID.
        load_shp:        Load a shapefile into a PostGIS table.
        load_shp_map:    Load multiple shapefiles into PostGIS tables.

    Attributes:
        database:        PostgreSQL database connection manager class.
        directory:       Path to the directory containing the shapefiles.
        srid:            Default Spatial Reference System Identifier (SRID).

    Attributes can be passed as additional constructor arguments and override
    configuration.

    Constructor arguments:
        config_filename: Path to additional configuration file.
                         If None, configuration must be provided in default
                         locations, or attributes must be passed as
                         constructor arguments in place of configuration.

    """

    def __init__(self, config_filename=None, database=None, directory=None,
                 srid=None):
        # Attempt to load configuration.
        config = load_config(config_filename)

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
            db_config = dict(config.items('database'))
            database.connect(**db_config)

        # Assign arguments to class attributes.
        self.database = database
        if os.path.exists(directory):
            self.directory = directory
        else:
            raise IOError("Directory does not exist: %s" % directory)
        self.srid = int(srid)

    def close(self):
        """Close managed PostgreSQL connection(s)."""
        return self.database.close()

    def get_encoding(self, filename):
        """Identify shapefile attribute table encoding.

        Use encoding specified by cpg or cst file, before falling back to
        LATIN1.

        Args:
            filename: Shapefile, relative to the data directory.

        Returns:
            encoding: Character encoding (str).

        """
        # Read encoding from shapefile cpg and cst file.
        filepath = os.path.join(self.directory, filename)
        encoding = None
        for extension in ['.cpg', '.cst']:
            encoding_filepath = os.path.splitext(filepath)[0] + extension
            try:
                with open(encoding_filepath) as encoding_file:
                    encoding = encoding_file.read().strip()
                logger.debug("%s file reported %s encoding: %s"
                             % (extension, encoding, filename))
                break
            except IOError:
                continue

        if not encoding:
            # No encoding found. Fall back to LATIN1.
            encoding = "LATIN1"
            logger.debug("Assuming %s attribute encoding: %s"
                         % (encoding, filename))

        return encoding

    def get_srid(self, filename):
        """Identify shapefile EPSG SRID using GDAL and prj2EPSG API.

        Try to identify the SRID of a shapefile by reading the
        projection information of the prj file and matching it to an
        EPSG SRID. Try GDAL and prj2EPSG API in order before returning
        None if a match was not found.

        Args:
            filename: Shapefile, relative to the data directory.

        Returns:
            srid: SRID, if identified, otherwise None.

        """
        # Read projection information from shapefile prj file.
        filepath = os.path.join(self.directory, filename)
        prj_filepath = os.path.splitext(filepath)[0] + '.prj'
        try:
            with open(prj_filepath) as prj_file:
                wkt = prj_file.read()
        except IOError:
            logger.warn("Unable to open projection information: %s"
                        % filename)
            return

        # Attempt to identify EPSG SRID using GDAL.
        sr = osr.SpatialReference()
        sr.ImportFromESRI([wkt])
        res = sr.AutoIdentifyEPSG()
        if res == 0:
            # Successfully identified SRID.
            srid = int(sr.GetAuthorityCode(None))
            logger.debug("GDAL returned SRID %s: %s" % (srid, filename))
            return srid

        # Try querying prj2EPSG API.
        params = urlencode({'terms': wkt, 'mode': 'wkt'})
        resp = urlopen('http://prj2epsg.org/search.json?' + params)
        data = json.loads(resp.read())
        if data['exact']:
            # Successfully identified SRID.
            srid = int(data['codes'][0]['code'])
            logger.debug("prj2EPSG API returned SRID %s: %s"
                        % (srid, filename))
            return srid

        # Unable to identify SRID.
        logger.warn("Unable to identify SRID: %s" % filename)

    def load_shp(self, filename, table, srid=None, encoding=None,
                 drop=False, append=False):
        """Load a shapefile from the directory into a PostGIS table.

        This is a Python wrapper for shp2gpsql. shp2pgsql is spawned by
        subprocess. Commands generated by shp2pgsql are executed on a
        psycopg2 cursor object. For performance, the PostgreSQL "dump"
        format is used instead of the default "insert" SQL format.

        Args:
            filename: Shapefile, relative to the data directory.
            table:    PostGIS table name (optionally schema-qualified).
            srid:     Spatial Reference System Identifier (SRID).
                      If None, attempt to identify SRID from projection
                      information before falling back to default.
            encoding: Shapefile attribute table encoding.
                      If None, attempt to identify encoding from cpg or cst
                      file before falling back to default.
            drop:     Whether to drop a table that already exists.
                      Defaults to False.
            append:   Whether to append to an existing table, instead of
                      creating one. Defaults to False.

        """
        filepath = os.path.join(self.directory, filename)

        # Make sure that shapefile exists and is readable.
        with open(filepath):
            pass

        # If SRID not provided, try to identify from projection information
        # before falling back to default SRID.
        if not srid:
            srid = self.get_srid(filename)
            if not srid:
                logger.warn("Falling back to default SRID %s: %s"
                            % (self.srid, filename))
                srid = self.srid

        # If encoding not provided, try to identify from cpg or cst file
        # before falling back to default encoding.
        if not encoding:
            encoding = self.get_encoding(filename)

        logger.info("Loading table %s (SRID: %s) from file %s (encoding: %s)."
                    % (table, srid, filename, encoding))
        with self.database.cursor() as cur:

            if drop:
                # Drop the existing table.
                cur.execute('DROP TABLE IF EXISTS %s' % table)

            if not append:
                # Create the new table itself without adding actual data.
                create_table = subprocess.Popen(['shp2pgsql', '-p', '-I',
                                                 '-s', str(srid),
                                                 '-W', encoding,
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
                    logf(logging.WARN, create_table.stderr)
                create_table.wait()

            # Append data to existing or newly-created table.
            append_data = subprocess.Popen(['shp2pgsql', '-a', '-D', '-I',
                                            '-s', str(srid),
                                            '-W', encoding,
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
                logf(logging.WARN, append_data.stderr)
            append_data.wait()

    def load_shp_map(self, mapping):
        """Load multiple shapefiles by mapping tables to filenames or kwargs.

        The shapefile dictionary should map each database table name to:

            - a shapefile filename to load, or
            - dict-like keyword arguments to pass to the load_shp method,
              other than the table name.

        By default, existing tables will be dropped (drop=True).

        """
        for (table, value) in mapping.items():
            if isinstance(value, basestring):
                self.load_shp(filename=value, table=table, drop=True)
            else:
                if 'drop' not in value:
                    value['drop'] = True
                self.load_shp(table=table, **value)


def load_multiple_shp(shapefiles, config_filename=None):
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
    config_filename : str, optional
        Path to additional configuration file.
        If None, configuration must be provided in default locations.
        Configuration should specify the input data directory (data_dir).
        The data_dir should contain subdirectories corresponding to each
        shapefile category, which in turn should contain a subdirectory
        for each shapefile.

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

    loader = DataLoader(config_filename)

    for shape_category in shapefiles:
        path_func = subpath(shape_category)
        shp_dict = shapefiles[shape_category]
        for shp_name in shp_dict:
            print 'Loading %s.' % shp_name
            path = path_func(shp_name, shp_dict[shp_name][0])
            loader.load_shp(filename=path,
                table=shape_category + '_' + shp_name,
                srid=shp_dict[shp_name][1], drop=True)
