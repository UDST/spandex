import json
import logging
import os
import subprocess

import pandas as pd
import psycopg2
from six import string_types
from six.moves import urllib
from sqlalchemy import func
from sqlalchemy.ext.declarative import DeclarativeMeta
from sqlalchemy.orm import Query

from .database import database as db, CreateTableAs
from .utils import load_config, logf


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


class TableLoader(object):
    """Table loader class with support for shapefiles and GeoAlchemy.

    Some example usage:

        loader = TableLoader()

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
        tables:          PostgreSQL table objects, namespaced by schema.

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
        self.tables = self.database.tables
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
            if isinstance(value, string_types):
                self.load_shp(filename=value, table=table, drop=True)
            else:
                if 'drop' not in value:
                    value['drop'] = True
                self.load_shp(table=table, **value)


def update_df(df, column, table):
    """
    Add or update column in DataFrame from database table.

    Database table must contain column with the same name as
    DataFrame's index (df.index.name).

    Parameters
    ----------
    df : pandas.DataFrame
        DataFrame to return an updated copy of.
    column : sqlalchemy.orm.attributes.InstrumentedAttribute
        Column ORM object to update DataFrame with.
    table : sqlalchemy.ext.declarative.DeclarativeMeta
        Table ORM class containing columns to update with and index on.

    Returns
    -------
    df : pandas.DataFrame

    """
    # Get table column to use as index based on DataFrame index name.
    index_column = getattr(table, df.index.name)

    # Query index column and column to update DataFrame with.
    with db.session() as sess:
        q = sess.query(index_column, column)

    # Update DataFrame column.
    new_df = db_to_df(q, index_name=df.index.name)
    df[column.name] = new_df[column.name]
    return df


def add_column(table, column_name, type_name, default=None):
    """
    Add column to table.

    Parameters
    ----------
    table : sqlalchemy.ext.declarative.DeclarativeMeta
        Table ORM class to add column to.
    column_name : str
        Name of column to add to table.
    type_name : str
        Name of column type.
    default : str, optional
        Default value for column. Must include quotes if string.

    Returns
    -------
    column : sqlalchemy.orm.attributes.InstrumentedAttribute
        Column ORM object that was added.

    """
    if default:
        default_str = "DEFAULT {}".format(default)
    else:
        default_str = ""

    t = table.__table__
    with db.cursor() as cur:
        cur.execute("""
            ALTER TABLE {schema}.{table}
            ADD COLUMN {column} {type} {default_str};
        """.format(
            schema=t.schema, table=t.name,
            column=column_name, type=type_name, default_str=default_str))
    db.refresh()
    return getattr(table, column_name)


def remove_column(column):
    """Remove column from table."""
    col = column.property.columns[0]
    t = col.table
    with db.cursor() as cur:
        cur.execute("""
            ALTER TABLE {schema}.{table}
            DROP COLUMN {column};
        """.format(schema=t.schema, table=t.name, column=col.name))
    db.refresh()


def exec_sql(query, params=None):
    """Execute SQL query."""
    with db.cursor() as cur:
        cur.execute(query, params)


def db_to_query(orm):
    """Convert table or list of ORM objects to a query."""
    if isinstance(orm, Query):
        # Assume input is Query object.
        return orm
    elif hasattr(orm, '__iter__'):
        # Assume input is list of ORM objects.
        with db.session() as sess:
            return sess.query(*orm)
    else:
        # Assume input is single ORM object.
        with db.session() as sess:
            return sess.query(orm)


def db_to_db(query, name, schema=None, view=False):
    """
    Create a table or view from Query, table, or ORM objects, like columns.

    Do not use to duplicate a table. The new table will not contain
    indexes or constraints, including primary keys.

    Parameters
    ----------
    query : sqlalchemy.orm.Query, sqlalchemy.ext.declarative.DeclarativeMeta,
            or iterable
        Query ORM object, table ORM class, or list of ORM objects to query,
        like columns.
    name : str
        Name of table or view to create.
    schema : schema class, optional
        Schema of table to create. Defaults to public.
    view : bool, optional
        Whether to create a view instead of a table. Defaults to False.

    Returns
    -------
    None

    """
    if schema:
        schema_name = schema.__name__
    else:
        schema_name = 'public'
    qualified_name = schema_name + "." + name

    q = db_to_query(query)

    # Create new table from results of the query.
    with db.session() as sess:
        sess.execute(CreateTableAs(qualified_name, q, view))
    db.refresh()


def db_to_df(query, index_name=None):
    """
    Return DataFrame from Query, table, or ORM objects, like columns.

    Parameters
    ----------
    query : sqlalchemy.orm.Query, sqlalchemy.ext.declarative.DeclarativeMeta,
            or iterable
        Query ORM object, table ORM class, or list of ORM objects to query,
        like columns.
    index_name : str, optional
        Name of column to use as DataFrame index. If provided, column
        must be contained in query.

    Returns
    -------
    df : pandas.DataFrame

    """
    q = db_to_query(query)

    # Convert Query object to DataFrame.
    entities = q.column_descriptions
    if (len(entities) == 1 and
            isinstance(entities[0]['type'], DeclarativeMeta)):
        # If we query a table, column_descriptions refers to the table itself,
        # not its columns.
        table = q.column_descriptions[0]['type']
        column_names = table.__table__.columns.keys()
    else:
        column_names = [desc['name'] for desc in q.column_descriptions]
    data = [rec.__dict__ for rec in q.all()]
    df = pd.DataFrame.from_records(data, index=index_name,
                                   columns=column_names, coerce_float=True)
    return df


def vacuum(table):
    """
    VACUUM and then ANALYZE table.

    VACUUM reclaims storage from deleted or obselete tuples.
    ANALYZE updates statistics used by the query planner to determine the most
    efficient way to execute a query.

    Parameters
    ----------
    table : sqlalchemy.ext.declarative.DeclarativeMeta
        Table ORM class to vacuum.

    Returns
    -------
    None

    """
    # Vacuum
    t = table.__table__
    with db.connection() as conn:
        assert conn.autocommit is False
        conn.autocommit = True
        try:
            with conn.cursor() as cur:
                cur.execute("VACUUM ANALYZE {schema}.{table};".format(
                    schema=t.schema, table=t.name))
        finally:
            conn.autocommit = False
