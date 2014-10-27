import json
import logging
import os
import subprocess

import psycopg2
import six
from six.moves import configparser, urllib
from sqlalchemy import func

from .database import database as db


# Set up logging system.
logging.basicConfig()
logger = logging.getLogger(__name__)


# Import from GDAL if available.
try:
    from osgeo import osr
except ImportError:
    logger.warn("GDAL bindings not available. No custom projection support.")
    gdal = False
else:
    gdal = True


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


class DataLoader(object):
    """Data loader class with support for shapefiles and GeoAlchemy.

    Some example usage:

        loader = DataLoader()

        # Load shapefile.
        loader.load_shp('parcel/Alameda.shp', 'staging.alameda')

        # Load multiple shapefiles.
        loader.load_shp_map({'staging.alameda': 'parcel/Alameda.shp'})

        # Run SQL command.
        with loader.database.cursor() as cur:
            cur.execute("SELECT DISTINCT luc_desc FROM staging.alameda;")
            for desc in cur:
                print(desc)

        # Run ORM command.
        session = loader.database.session
        alameda = loader.database.tables.staging.alameda
        for desc in session.query(alameda.luc_desc).distinct():
            print(desc)

        # Refresh ORM if schema was modified.
        loader.database.refresh()

        # Close all connection(s).
        loader.close()

    Methods:
        duplicate:       Duplicate a PostgreSQL table, including indexes.
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

        # Define arguments from configuration, unless overridden.
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

        # Assign arguments to attributes.
        self.database = database
        if os.path.exists(directory):
            self.directory = directory
        else:
            raise IOError("Directory does not exist: %s" % directory)
        self.srid = int(srid)

    def duplicate(self, table, new_table_name, schema_name='public'):
        """
        Duplicate a PostgreSQL table, including indexes and constraints.

        Parameters
        ----------
        table : sqlalchemy.ext.declarative.api.DeclarativeMeta
            Table ORM class to duplicate.
        new_table_name : str
            Name of new table.
        schema_name : str, optional
            Name of schema to contain the new table. Default is 'public'.

        Returns
        -------
        new_table : sqlalchemy.ext.declarative.api.DeclarativeMeta
            Duplicated ORM table class.

        """
        # Copy schema including constraints and indexes, then insert values.
        # This may be inefficient, unfortunately.
        t = table.__table__
        with db.cursor() as cur:
            cur.execute("""
                CREATE TABLE {nschema}.{ntable}
                    (LIKE {oschema}.{otable} INCLUDING ALL);
                INSERT INTO {nschema}.{ntable}
                    SELECT * FROM {oschema}.{otable};
            """.format(nschema=schema_name, ntable=new_table_name,
                       oschema=t.schema, otable=t.name))

        # Refresh ORM and return table class.
        db.refresh()
        return getattr(getattr(db.tables, schema_name), new_table_name)

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

        if not encoding or encoding.lower() == "system":
            # No encoding found. Fall back to LATIN1.
            encoding = "LATIN1"
            logger.debug("Assuming %s attribute encoding: %s"
                         % (encoding, filename))

        return encoding

    def get_srid(self, filename):
        """Identify shapefile SRID using GDAL and prj2EPSG API.

        Try to identify the SRID of a shapefile by reading the
        projection information of the prj file and matching to an
        EPSG SRID using GDAL and the prj2EPSG API.

        If the prj file cannot be read, warn and return 0,
        which is the default SRID in PostGIS 2.0+.

        If no match is found, define a custom projection in the
        spatial_ref_sys table and return its SRID.

        Args:
            filename: Shapefile, relative to the data directory.

        Returns:
            srid: EPSG, custom SRID, or 0.

        """
        # Read projection information from shapefile prj file.
        filepath = os.path.join(self.directory, filename)
        prj_filepath = os.path.splitext(filepath)[0] + '.prj'
        try:
            with open(prj_filepath) as prj_file:
                wkt = prj_file.read().strip()
        except IOError:
            logger.warn("Unable to open projection information: %s"
                        % filename)
            return 0

        # Attempt to identify EPSG SRID using GDAL.
        if gdal:
            sr = osr.SpatialReference()
            sr.ImportFromESRI([wkt])
            res = sr.AutoIdentifyEPSG()
            if res == 0:
                # Successfully identified SRID.
                srid = int(sr.GetAuthorityCode(None))
                logger.debug("GDAL returned SRID %s: %s" % (srid, filename))
                return srid

        # Try querying prj2EPSG API.
        params = urllib.parse.urlencode({'terms': wkt, 'mode': 'wkt'})
        resp = urllib.request.urlopen('http://prj2epsg.org/search.json?'
                                      + params)
        data = json.load(resp)
        if data['exact']:
            # Successfully identified SRID.
            srid = int(data['codes'][0]['code'])
            logger.debug("prj2EPSG API returned SRID %s: %s"
                         % (srid, filename))
            return srid

        # Unable to identify EPSG SRID. Use custom SRID.
        srs = self.database.tables.public.spatial_ref_sys
        with self.database.session() as sess:
            srid = sess.query(srs.srid).filter(srs.srtext == wkt).first()
        if srid:
            return srid[0]
        else:
            if gdal:
                # Need to define custom projection since not in database.
                logger.warn("Defining custom projection: %s" % filename)
                proj4 = sr.ExportToProj4().strip()
                if not proj4:
                    raise RuntimeError("Unable to project: %s" % filename)
                with self.database.session() as sess:
                    srid = sess.query(func.max(srs.srid)).one()[0] + 1
                    projection = srs(srid=srid,
                                     auth_name="custom", auth_srid=srid,
                                     srtext=wkt, proj4text=proj4)
                    sess.add(projection)
                srid = projection.srid
            else:
                raise RuntimeError("No GDAL: unable to define projection.")
        logger.debug("Using custom SRID %s: %s" % (srid, filename))
        return srid

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

        # If SRID not provided, identify from projection information.
        if not srid:
            srid = self.get_srid(filename)

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
                    command = b""
                    for line in create_table.stdout:
                        if line and not (line.startswith(b"BEGIN") or
                                         line.startswith(b"COMMIT")):
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
                    if line.startswith(b"COPY"):
                        break
                cur.copy_expert(line, append_data.stdout)
            finally:
                logf(logging.WARN, append_data.stderr)
            append_data.wait()

        # Refresh ORM.
        self.database.refresh()

    def load_shp_map(self, mapping):
        """Load multiple shapefiles by mapping tables to filenames or kwargs.

        The shapefile dictionary should map each database table name to:

            - a shapefile filename to load, or
            - dict-like keyword arguments to pass to the load_shp method,
              other than the table name.

        By default, existing tables will be dropped (drop=True).

        """
        for (table, value) in mapping.items():
            if isinstance(value, six.string_types):
                self.load_shp(filename=value, table=table, drop=True)
            else:
                if 'drop' not in value:
                    value['drop'] = True
                self.load_shp(table=table, **value)
