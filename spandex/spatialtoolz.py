import numpy as np
import pandas as pd
import pandas.io.sql as sql

from .database import database as db
from .utils import DataLoader


def tag(target_table_name, target_field, source_table_name, source_table_field,
        how='point_in_poly', target_df=None):
    """
    Tags target table with attribute of another table based on
    spatial relationship.

    Parameters
    ----------
    target_table_name : str
        Name of target table to be tagged.
    target_field : str
        Name of field in target table to add (if doesn't exist)
        or update (if exists).
    source_table_name : str
        Name of source table containing information to tag target table with.
    source_table_field : str
        Name of field in source table that contains the information.
    how : str, optional
        How to relate the two tables spatially.
        If not specified, defaults to 'point_in_poly'
    target_df : DataFrame, optional
        DataFrame to return a tagged copy of.

    Returns
    -------
    None : None
        Field is added or updated on the target_table in the database,
        and returns nothing. Unless target_df argument is used,
        in which case return value is pandas.DataFrame with
        the new/updated column.

    """
    check_srid_equality(target_table_name, source_table_name)

    if db_col_exists(target_table_name, target_field) is False:
        add_integer_field(target_table_name, target_field)

    if how == 'point_in_poly':
        if db_col_exists(target_table_name, 'centroid') is True:
            exec_sql(
                ("update {tname} set {tfield} = b.{sfield} "
                 "from {sname} b "
                 "where st_within({tname}.centroid, b.geom)"
                 ).format(
                    tname=target_table_name, tfield=target_field,
                    sfield=source_table_field, sname=source_table_name))
        else:
            exec_sql(
                ("update {tname} set {tfield} = b.{sfield} "
                 "from {sname} b "
                 "where st_within(ST_centroid({tname}.geom), b.geom)"
                 ).format(
                    tname=target_table_name, tfield=target_field,
                    sfield=source_table_field, sname=source_table_name))

    if target_df:
        return update_df(target_df, target_field, target_table_name)


def proportion_overlap(
        target_table_name, overlapping_table_name, target_field,
        target_df=None):
    """
    Calculates proportion overlap between target table's geometry and another
    table's geometry. Populates field in target table with proportion
    overlap value.

    Parameters
    ----------
    target_table_name : str
        Name of target table being overlapped.
    overlapping_table_name : str
        Name of table containing geometry that overlaps with target
        table's geometry.
    target_field : str
        Name of field in target table to add (if doesn't exist) or
        update (if exists). This is where proportion overlap value
        will be stored.
    target_df : DataFrame, optional
        DataFrame to return a copy of with proportion overlap calculation.

    Returns
    -------
    None : None
        Field is added or updated on the target_table in the database,
        and returns nothing. Unless target_df argument is used,
        in which case return value is pandas.DataFrame
        with the new/updated column.

    """
    check_srid_equality(target_table_name, overlapping_table_name)

    if db_col_exists(target_table_name, target_field) is False:
        add_numeric_field(target_table_name, target_field)

    calc_area(target_table_name)

    exec_sql(
        ("UPDATE {tname} "
         "SET {tfield} = (SELECT SUM(ST_Area(ST_Intersection({tname}.geom, {oname}.geom))) "
         "FROM {oname} "
         "WHERE ST_Intersects({tname}.geom, {oname}.geom)) / {tname}.calc_area;"
         ).format(
            tname=target_table_name, tfield=target_field,
            oname=overlapping_table_name))

    if target_df:
        return update_df(target_df, target_field, target_table_name)


def get_srid(table_name, field):
    """Returns SRID of specified table/field."""
    try:
        return db_to_df(
            "SELECT FIND_SRID('public', '{tname}', '{field}')".format(
                tname=table_name, field=field)).values[0][0]
    except:
        pass


def srid_equality(target_table_name, source_table_name):
    """Checks if there are multiple projections between two tables."""
    srids = []

    def check_append_srid(table_name, field_name):
        if db_col_exists(table_name, field_name):
            srids.append(get_srid(table_name, field_name))

    check_append_srid(target_table_name, 'geom')
    check_append_srid(source_table_name, 'geom')
    check_append_srid(target_table_name, 'centroid')
    check_append_srid(source_table_name, 'centroid')
    srids = np.unique(srids)
    return False if len(srids[srids > 0]) > 1 else True


def check_srid_equality(table1, table2):
    """
    Tests for SRID equality between two tables and raises Exception if unequal.

    """
    if srid_equality(table1, table2) is False:
        raise Exception('Projections are different')


def calc_area(table_name):
    """
    Calculates area of geometry using ST_Area, values stored in
    'calc_area' field.

    """
    if db_col_exists(table_name, 'calc_area') is False:
        add_numeric_field(table_name, 'calc_area')
        exec_sql(
            "UPDATE {tname} SET calc_area = ST_Area({tname}.geom);".format(
                tname=table_name))


def invalid_geometry_diagnostic(table_name, id_field):
    """"""
    """
    Returns DataFrame with diagnostic information for only records
    with invalid geometry. Returned columns include record identifier,
    whether geometry is simple, and reason for invalidity.

    Parameters
    ----------
    table_name : str
        Name of database table to diagnose.
    id_field : str
        Name of unique identifier field in database table.  Can be any field.

    Returns
    -------
    df : pandas.DataFrame
        Table with all records that have invalid geometry, with
        diagnostic information.

    """
    return db_to_df(
        ("SELECT * FROM ("
         "SELECT {field}, ST_IsValid(geom) as valid, "
         "ST_IsSimple(geom) as simple,  ST_IsValidReason(geom), geom FROM {tname}"
         ") AS t WHERE NOT(valid);").format(field=id_field, tname=table_name))


def duplicate_stacked_geometry_diagnostic(table_name):
    """
    Returns DataFrame with all records that have duplicate, stacked geometry.

    Parameters
    ----------
    table_name : str
        Name of database table to diagnose.

    Returns
    -------
    df : pandas.DataFrame
        Table with all records that have duplicate, stacked geometry.

    """
    return db_to_df(
        ("SELECT * FROM {tname} "
         "where geom in (select geom from {tname} "
         "group by geom having count(*) > 1)").format(tname=table_name))


def update_df(df, column_name, db_table_name):
    """
    Adds/updates column in DataFrame from database table.
    Database table must contain field with the same name as
    DataFrame's index (df.index.name).

    Parameters
    ----------
    df : DataFrame
        Table to add column to.
    column_name : str
        Name of field in database table to add to DataFrame.
        This is also the name of the column to add/update in the DataFrame.
    db_table_name : str
        Database table containing field to add/update DataFrame.

    Returns
    -------
    df : pandas.DataFrame
        Table with new/updated column.

    """
    df_idx_name = df.index.name
    new_col = db_to_df(
        "select {idx}, {col} from {tname}".format(
            idx=df_idx_name, col=column_name, tname=db_table_name)
        ).set_index(df_idx_name)[column_name]
    df[column_name] = new_col
    return df


def db_col_exists(table_name, column_name):
    """Tests if column on database table exists"""
    test = db_to_df(
        ("SELECT column_name "
         "FROM information_schema.columns "
         "WHERE table_name='{tname}' and column_name='{col}';").format(
            tname=table_name, col=column_name))

    return True if len(test) > 0 else False


def add_integer_field(table_name, field_to_add):
    """Add integer field to table."""
    exec_sql(
        "alter table {tname} add {field} integer default 0;".format(
            tname=table_name, field=field_to_add))


def add_numeric_field(table_name, field_to_add):
    """Add numeric field to table."""
    exec_sql(
        "alter table {tname} add {field} numeric default 0.0;".format(
            tname=table_name, field=field_to_add))


def exec_sql(query, params=None):
    """Executes SQL query."""
    with db.cursor() as cur:
        cur.execute(query, params)


def db_to_df(query, params=None):
    """Executes SQL query and returns DataFrame."""
    with db.connection() as conn:
        return sql.read_sql(query, conn, params=params)


def reproject(
        target_table, config_dir, geometry_column='geom', new_table=None):
    """
    Reprojects target table into the srid specified in the project config

    Parameters
    ----------
    target_table: str
        Name of table to reproject.  Default is in-place reprojection.
    config_dir : str
        Path to the directory where the project config is stored.
    geometry_column : str, optional
        Name of the geometry column in the target table. Default is 'geom'.
    new_table: str, optional
        If `new_table` is specified, a copy of target table is made with
        name `new_table`.

    Returns
    -------
    None
        Target table's geometry column is reprojected to the SRID found
        in the config file. Function detects current target table SRID and
        project SRID and converts on the database.

    """
    project_srid = str(DataLoader(config_dir).srid)
    table_srid = str(get_srid(target_table, geometry_column))

    def update_srid(target_table, geometry_column, table_srid, project_srid):
        exec_sql(
            "SELECT UpdateGeometrySRID('{table}', '{col}', {srid})".format(
                table=target_table, col=geometry_column, srid=project_srid))
        exec_sql(
            ("UPDATE {table} "
             "SET {col} = ST_TRANSFORM(ST_SetSRID({col}, {tsrid}), {psrid})"
             ).format(
                 table=target_table, col=geometry_column, tsrid=table_srid,
                 psrid=project_srid))

    if new_table:
        exec_sql("CREATE TABLE {ntable} as SELECT * FROM {ttable}".format(
            ntable=new_table, ttable=target_table))
        update_srid(new_table, geometry_column, table_srid, project_srid)
        exec_sql(
            ("CREATE INDEX {ntable}_{col}_gist on "
             "{ntable} using gist({col})"
             ).format(
                 ntable=new_table, col=geometry_column))
        vacuum(new_table)
    else:
        update_srid(target_table, geometry_column, table_srid, project_srid)
        vacuum(target_table)


def vacuum(target_table):
    """vacuums target table, returning stats for indices, etc."""
    with db.connection() as conn:
        conn.set_isolation_level(0)
    exec_sql("VACUUM ANALYZE {table}".format(table=target_table))


def conform_srids(config_dir, schema=None):
    """
    Reprojects all non-conforming geometry columns into project SRID.

    Parameters
    ----------
    config_dir : str
        Path to the directory where the project config is stored.
    schema : str, optional
        If schema is specified, only SRIDs within specified schema
        are conformed.

    Returns
    -------
    None
        Nonconforming tables' geometry columns are reprojected to the SRID
        found in the config file.

    """
    geoms = db_to_df(
        "select f_table_schema, f_table_name, f_geometry_column, srid "
        "from geometry_columns;")
    if schema:
        geoms = geoms[geoms.f_table_schema == schema]
    project_srid = DataLoader(config_dir).srid
    geoms = geoms[geoms.srid != project_srid]
    for item in geoms.index:
        target_table = geoms.f_table_name[geoms.index == item]
        geom_col = geoms.f_geometry_column[geoms.index == item]
        reproject(
            target_table[item], config_dir, geometry_column=geom_col[item])


def load_delimited_file(file_path, table_name, delimiter=',', append=False):
    """
    Load a delimited file to the database.

    Parameters
    ----------
    file_path : str
        The full path to the delimited file.
    table_name : str
        The name given to the table on the database or the table to append to.
    delimiter : str, optional
        The delimiter symbol used in the input file. Defaults to ','.
        Other examples include tab delimited '\t' and
        vertical bar delimited '|'.
    append: boolean, optional
        Determines whether a new table is created (dropping existing table
        if exists) or rows are appended to existing table.
        If append=True, table schemas must be identical.

    Returns
    -------
    None
        Loads delimited file to database

    """
    delimited_file = pd.read_csv(file_path, delimiter=delimiter)
    dtypes = pd.Series(list(delimited_file.dtypes))
    dtypes[dtypes == 'object'] = 'character varying'
    dtypes[dtypes == 'int64'] = 'integer'
    dtypes[dtypes == 'int32'] = 'integer'
    dtypes[dtypes == 'float64'] = 'numeric'
    cols = pd.Series(list(delimited_file.columns))
    cols = cols.str.replace(' ', '_')
    cols = cols.str.replace('\'', '')
    cols = cols.str.replace('\"', '')
    cols = cols.str.replace('\(', '')
    cols = cols.str.replace('\)', '')
    cols = cols.str.replace('\+', '')
    cols = cols.str.replace('\:', '')
    cols = cols.str.replace('\;', '')
    columns = ''
    for col, tp in zip(list(cols), list(dtypes)):
        columns = columns + col + ' ' + tp + ','
    columns = columns[:-1]
    if not append:
        exec_sql("DROP TABLE IF EXISTS {table};".format(table=table_name))
        exec_sql("CREATE TABLE {table} ({cols});".format(
            table=table_name, cols=columns))
    exec_sql("SET CLIENT_ENCODING='LATIN1';")
    exec_sql(
        "COPY {table} FROM '{file}' DELIMITER '{delim}' CSV HEADER;".format(
            table=table_name, file=file_path, delim=delimiter))
