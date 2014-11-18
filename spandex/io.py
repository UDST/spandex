import json
import logging
import os
import subprocess

import pandas as pd
import psycopg2
from six import string_types
from six.moves import cStringIO, urllib
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
        alameda = loader.tables.staging.alameda
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

    def get_path(self, filename):
        """
        Get the absolute path to a file in the data directory.

        Parameters
        ----------
        filename: str
            File path, relative to the data directory.

        Returns
        -------
        filepath : str
            Absolute file path.

        """
        filepath = os.path.join(self.directory, filename)
        return filepath

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
        filepath = self.get_path(filename)
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
        filepath = self.get_path(filename)
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
        srs = self.tables.public.spatial_ref_sys
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
        filepath = self.get_path(filename)

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


class TableFrame(object):
    """
    DataFrame-like object for read-only access to a database table.

    TableFrame wraps a SQLAlchemy ORM table for queries using syntax
    similar to key and attribute access on a pandas DataFrame.
    These DataFrame-like operations are supported:

        my_tableframe = TableFrame(table, index_name='gid')
        my_series1 = my_tableframe['col1']
        my_dataframe = my_tableframe[['col1', 'col2']]
        my_series2 = my_tableframe.col2
        num_rows = len(my_tableframe)

    Caching is enabled by default. As columns are queried, they will
    be cached as individual Series objects for future lookups.
    The cache can be emptied by calling the `clear` method.
    Caching can be enabled and disabled with the `cache` parameter or
    by reassigning to the `cache` attribute.

    Unlike a DataFrame, TableFrame is read-only.

    TableFrame instances can be registered as tables in the
    UrbanSim simulation framework using the `sim.add_table` function.

    Parameters
    ----------
    table : sqlalchemy.ext.declarative.DeclarativeMeta
        Table ORM class to wrap.
    index_name : str
        Name of column to use as DataFrame and Series index.
    cache : bool
        Whether to cache columns as they are queried.

    Attributes
    ----------
    cache : bool
        Whether caching is enabled. Can be reassigned to enable/disable.
    columns : list of str
        List of column names in database table.
    index : pandas.Index
        DataFrame and Series index

    """
    def __init__(self, table, index_name=None, cache=False):
        super(TableFrame, self).__init__()
        super(TableFrame, self).__setattr__('_table', table)
        super(TableFrame, self).__setattr__('_index_name', index_name)
        super(TableFrame, self).__setattr__('cache', cache)
        super(TableFrame, self).__setattr__('_cached', {})
        super(TableFrame, self).__setattr__('_index', pd.Index([]))

    @property
    def columns(self):
        return self._table.__table__.columns.keys()

    @property
    def index(self):
        if not self.cache or len(self._index) == 0:
            if self._index_name:
                index_column = getattr(self._table, self._index_name)
                index = db_to_df(index_column,
                                 index_name=self._index_name).index
            else:
                self._index = pd.Index(range(len(self)))
            super(TableFrame, self).__setattr__('_index', index)
        return self._index

    def clear(self):
        """Clear column cache."""
        self._cached.clear()

    def copy(self):
        """Object is read-only, so the same object is returned."""
        return self

    def __dir__(self):
        """Support IPython tab-completion of column names."""
        return self.__dict__.keys() + self.columns

    def __getitem__(self, key):
        """
        Return column(s) as a pandas Series or DataFrame.

        Like pandas, if the key is an iterable, a DataFrame will be
        returned containing the column(s) named in the iterable.
        Otherwise a Series will be returned.

        """
        # Collect column name(s).
        if hasattr(key, '__iter__') and not isinstance(key, string_types):
            # Argument is list-like. Later return DataFrame.
            return_dataframe = True
            column_names = key
        else:
            # Argument is scalar. Later return Series.
            return_dataframe = False
            column_names = [key]

        if self.cache:
            # Collect cached columns and exclude from query.
            cached = []
            query_columns = []
            for name in column_names:
                if name in self._cached:
                    cached.append(self._cached[name])
                else:
                    query_columns.append(getattr(self._table, name))
        else:
            # Caching disabled, so query all columns.
            query_columns = [getattr(self._table, n) for n in column_names]

        if query_columns:
            # Query uncached columns including column used as index.
            query_columns.append(self._index_name)
            query_df = db_to_df(query_columns, index_name=self._index_name)
            if self.cache:
                # Join queried columns to cached columns.
                df = pd.concat([query_df] + cached, axis=1, copy=False)
                for (column_name, series) in query_df.iteritems():
                    self._cached[column_name] = series
            else:
                # Caching disabled, so no join.
                df = query_df
        else:
            # All columns were cached, so join them.
            df = pd.concat(cached, axis=1, copy=False)

        if return_dataframe:
            # Return DataFrame with specified column order.
            return df.reindex_axis(column_names, axis=1, copy=False)
        else:
            # Return Series.
            return df[key]

    def __getattr__(self, key):
        """Return column as a pandas Series."""
        return self.__getitem__(key)

    def __len__(self):
        """Calculate length from number of rows in database table."""
        with db.session() as sess:
            return sess.query(self._table).count()

    def __setattr__(self, name, value):
        """No attribute assignment, except to enable/disable cache."""
        if name == 'cache':
            super(TableFrame, self).__setattr__('cache', value)
        else:
            raise TypeError("TableFrame is read-only.")


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
    elif hasattr(orm, '__iter__') and not isinstance(orm, string_types):
        # Assume input is list of ORM objects.
        with db.session() as sess:
            return sess.query(*orm)
    else:
        # Assume input is single ORM object.
        with db.session() as sess:
            return sess.query(orm)


def db_to_db(query, table_name, schema=None, view=False, pk='id'):
    """
    Create a table or view from Query, table, or ORM objects, like columns.

    Do not use to duplicate a table. The new table will not contain
    the same indexes or constraints.

    Parameters
    ----------
    query : sqlalchemy.orm.Query, sqlalchemy.ext.declarative.DeclarativeMeta,
            or iterable
        Query ORM object, table ORM class, or list of ORM objects to query,
        like columns.
    table_name : str
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
    qualified_name = schema_name + "." + table_name

    q = db_to_query(query)

    # Create new table from results of the query.
    with db.session() as sess:
        sess.execute(CreateTableAs(qualified_name, q, view))
        if pk:
            sess.execute("""
                ALTER TABLE {} ADD COLUMN {} serial PRIMARY KEY;
            """.format(qualified_name, pk))
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


def df_to_db(df, table_name, schema=None, pk='id'):
    if schema:
        schema_name = schema.__name__
        qualified_name = "{}.{}".format(schema_name, table_name)
    else:
        schema_name = None
        qualified_name = table_name
    empty_df = df.iloc[[0]]
    with db.cursor() as cur:
        empty_df.to_sql(table_name, db._engine, schema=schema_name,
                        index=True, if_exists='replace')
        cur.execute("DELETE FROM {}".format(qualified_name))
        buf = cStringIO()
        df.to_csv(buf, sep='\t', na_rep=r'\N', header=False, index=True)
        buf.seek(0)
        cur.copy_from(buf, qualified_name,
                      columns=tuple([df.index.name] +
                                    df.columns.values.tolist()))
        if pk:
            cur.execute("""
                ALTER TABLE {} ADD COLUMN {} serial PRIMARY KEY;
            """.format(qualified_name, pk))
    db.refresh()


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
