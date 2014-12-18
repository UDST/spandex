import logging

from geoalchemy2 import Geometry
from sqlalchemy import func, or_
from sqlalchemy.orm import aliased, class_mapper

from . import io
from .database import database as db


"""Contains spatial functions."""


# Set up logging system.
logging.basicConfig()
logger = logging.getLogger(__name__)


def tag(target_table, target_column_name, source_table, source_column_name,
        how='point_in_poly', df=None):
    """
    Tag target table with attribute of a spatially-related source table.

    Parameters
    ----------
    target_table : sqlalchemy.ext.declarative.DeclarativeMeta
        Target table ORM class to be tagged.
    target_column_name : str
        Name of column in target table to add (if doesn't exist)
        or update (if exists). This where the tag value will be stored.
    source_table : sqlalchemy.ext.declarative.DeclarativeMeta
        Source table ORM class containing information to tag target table.
    source_column_name : str
        Name of column in source table that contains the tagging information.
    how : str, optional
        How to relate the two tables spatially.
        If not specified, defaults to 'point_in_poly'.
        Other spatial relationships are not currently supported.
    df : pandas.DataFrame, optional
        DataFrame to return a tagged copy of.

    Returns
    -------
    None
        However, if df argument is provided, pandas.DataFrame with the
        new or updated column is returned.

    """
    # Other spatial relationships are not supported.
    if how != "point_in_poly":
        raise ValueError("Only how='point_in_poly' is supported, not "
                         "how='{}',".format(how))

    # Table projections must be equal.
    assert srid_equality([target_table, source_table])

    # Get source column ORM object.
    source_column = getattr(source_table, source_column_name)

    # Add target column to target table if it does not already exist.
    if target_column_name in target_table.__table__.columns:
        target_column = getattr(target_table, target_column_name)
    else:
        # Use data type of source column for new column.
        dtype = source_column.property.columns[0].type.compile()
        target_column = io.add_column(target_table, target_column_name, dtype)

    # Tag target table with column from source table.
    with db.session() as sess:
        sess.query(target_table).filter(
            target_table.geom.ST_Centroid().ST_Within(source_table.geom)
        ).update(
            {target_column: source_column},
            synchronize_session=False
        )

    if df:
        return io.update_df(df, target_column, target_table)


def proportion_overlap(target_table, over_table, column_name, df=None):
    """
    Calculate proportion of target table geometry overlap.

    Calculate proportion of geometry area in each row of target table that
    is overlapped by another table's geometry. Populate specified column in
    target table with proportion overlap value.

    Parameters
    ----------
    target_table : sqlalchemy.ext.declarative.DeclarativeMeta
        Target table ORM class containing geometry to overlap.
    over_table : sqlalchemy.ext.declarative.DeclarativeMeta
        Table ORM class containing overlapping geometry.
    column_name : str
        Name of column in target table to add (if doesn't exist) or
        update (if exists). This is where the proportion overlap value
        will be stored.
    df : pandas.DataFrame, optional
        DataFrame to return a copy of with proportion overlap calculation.

    Returns
    -------
    None
        However, if df argument is provided, pandas.DataFrame with the
        new or updated column is returned.

    """
    # Table projections must be equal.
    assert srid_equality([target_table, over_table])

    # Add column to target table if it does not already exist.
    if column_name in target_table.__table__.columns:
        column = getattr(target_table, column_name)
    else:
        column = io.add_column(target_table, column_name, 'float')

    # Pre-calculate column area.
    calc_area(target_table)

    # Calculate proportion of overlapping area for each target table row.
    with db.session() as sess:
        proportion_overlap = sess.query(
            func.sum(
                target_table.geom.ST_Intersection(over_table.geom).ST_Area()
            ) / target_table.calc_area
        ).filter(
            target_table.geom.ST_Intersects(over_table.geom)
        ).group_by(
            target_table.geom
        )
        sess.query(target_table).update(
            {column: proportion_overlap.selectable},
            synchronize_session=False
        )

    if df:
        return io.update_df(df, column, target_table)


def trim(target_col, trim_col):
    """
    Trim target geometry by removing intersection with a trim column.

    Parameters
    ----------
    target_col : sqlalchemy.orm.attributes.InstrumentedAttribute
        Column ORM object to trim.
    trim_col : sqlalchemy.orm.attributes.InstrumentedAttribute
        Column ORM object to trim target column with.

    Returns
    -------
    None

    """
    # TODO: Aggregate multiple rows in trim_col.
    # Needs testing to make sure that ST_Difference can handle MultiPolygons
    # without data loss.
    with db.session() as sess:
        data_type = target_col.property.columns[0].type
        geom_type = data_type.geometry_type
        if geom_type.lower() == "multipolygon":
            # ST_Difference outputs Polygon, not MultiPolygon.
            # Temporarily change the geometry data type to generic Geometry.
            column_name = target_col.name
            table_name = target_col.parent.tables[0].name
            schema_name = target_col.parent.tables[0].schema
            srid = data_type.srid
            sess.execute("""
                ALTER TABLE {schema}.{table} ALTER COLUMN {column}
                SET DATA TYPE geometry(Geometry, {srid});
            """.format(
                schema=schema_name, table=table_name, column=column_name,
                srid=srid)
            )

        # Update column value with ST_Difference if ST_Intersects.
        table = target_col.parent
        sess.query(table).filter(
            target_col.ST_Intersects(trim_col)
        ).update(
            {target_col: target_col.ST_Difference(trim_col)},
            synchronize_session=False
        )

        if geom_type.lower() == "multipolygon":
            # Coerce the geometry data type back to MultiPolygon.
            sess.execute("""
                ALTER TABLE {schema}.{table} ALTER COLUMN {column}
                SET DATA TYPE geometry(MultiPolygon, {srid})
                USING ST_Multi(geom);
            """.format(
                schema=schema_name, table=table_name, column=column_name,
                srid=srid)
            )


def srid_equality(tables):
    """
    Check whether there is only one projection in list of tables.

    Parameters
    ----------
    tables: iterable
        List of table ORM classes to inspect geometry columns.

    Returns
    -------
    unique: boolean

    """
    # Iterate over all columns to build set of SRIDs.
    srids = set()
    for table in tables:
        for c in table.__table__.columns:
            if isinstance(c.type, Geometry):
                # Column is geometry column.
                srids.add(c.type.srid)

    # Projection is unique if set has single SRID.
    assert len(srids) > 0
    if len(srids) == 1:
        return True
    else:
        return False


def calc_area(table):
    """
    Calculate area in units of projection and store value in calc_area column.

    Parameters
    ----------
    table : sqlalchemy.ext.declarative.DeclarativeMeta
        Table ORM class with geom column to calculate area for. Value is
        stored in the calc_area column, which is created if it does not exist.

    Returns
    -------
    None

    """
    # Add calc_area column if it does not already exist..
    if 'calc_area' in table.__table__.columns:
        column_added = False
        column = table.calc_area
    else:
        column_added = True
        column = io.add_column(table, 'calc_area', 'float')

    # Calculate geometric area.
    try:
        with db.session() as sess:
            sess.query(table).update(
                {column: table.geom.ST_Area()},
                synchronize_session=False
            )
    except:
        # Remove column if it was freshly added and exception raised.
        if column_added:
            io.remove_column(column)
        raise


def calc_dist(table, geom):
    """
    Calculate distance between a table of geometries and a geometry column.

    Calculates the minimum Cartesian distance in units of projection between
    each geometry in the table and the nearest point in the geometry column.
    Geometries must have the same projection (SRID).

    Parameters
    ----------
    table : sqlalchemy.ext.declarative.DeclarativeMeta
        Table ORM class with geom column to calculate distance from. Value is
        stored in the calc_dist column, which is created if it does not exist.
    geom : sqlalchemy.orm.Query,
           sqlalchemy.orm.attributes.InstrumentedAttribute
        ORM object to calculate distance to, like a column or query.
        Must contain only one column. Rows are aggregated into a MULTI object
        with ST_Collect (faster union that does not dissolve boundaries).

    Returns
    -------
    column : sqlalchemy.orm.attributes.InstrumentedAttribute
        Column containing distances from the table to the geometry column.

    """
    # Add calc_dist column if it does not already exist..
    if 'calc_dist' in table.__table__.columns:
        column_added = False
        column = table.calc_dist
    else:
        column_added = True
        column = io.add_column(table, 'calc_dist', 'float')

    # Calculate geometric distance.
    try:
        with db.session() as sess:
            # Aggregate geometry column into single MULTI object.
            multi = sess.query(
                func.ST_Collect(
                    io.db_to_query(geom).label('geom')
                )
            )
            # Calculate distances from table geometries to MULTI object.
            sess.query(table).update(
                {column: table.geom.ST_Distance(multi)},
                synchronize_session=False
            )
        return column
    except:
        # Remove column if it was freshly added and exception raised.
        if column_added:
            io.remove_column(column)
        raise


def geom_invalid(table, index=None):
    """
    Return DataFrame with information on records with invalid geometry.

    Returned columns include record identifier, whether geometry is simple,
    and reason for invalidity.

    Parameters
    ----------
    table : sqlalchemy.ext.declarative.DeclarativeMeta
        Table ORM class to diagnose.
    index : sqlalchemy.orm.attributes.InstrumentedAttribute, optional
        Column ORM object to use as index.

    Returns
    -------
    df : pandas.DataFrame

    """
    # Build list of columns to return, including optional index.
    columns = [func.ST_IsValidReason(table.geom).label('reason'),
               table.geom]
    if index:
        columns.append(index)

    # Query information on rows with invalid geometries.
    with db.session() as sess:
        q = sess.query(
            *columns
        ).filter(
            ~table.geom.ST_IsValid()
        )

    # Convert query to DataFrame.
    if index:
        df = io.db_to_df(q, index_col=index.name)
    else:
        df = io.db_to_df(q)
    return df


def geom_duplicate(table):
    """
    Return DataFrame with all records that have identical, stacked geometry.

    Parameters
    ----------
    table : sqlalchemy.ext.declarative.DeclarativeMeta
        Table ORM class to diagnose.

    Returns
    -------
    df : pandas.DataFrame

    """
    # Create table aliases to cross join table to self.
    table_a = aliased(table)
    table_b = aliased(table)

    # Get primary key of table and table aliases.
    pk = class_mapper(table).primary_key[0]
    table_a_pk = getattr(table_a, pk.name)
    table_b_pk = getattr(table_b, pk.name)

    # Query rows with duplicate geometries.
    with db.session() as sess:
        dups = sess.query(
            table_a_pk.label('a'), table_b_pk.label('b')
        ).filter(
            table_a_pk < table_b_pk,
            func.ST_Equals(table_a.geom, table_b.geom)
        )
        rows = sess.query(table).filter(
            or_(
                pk.in_(dups.selectable.with_only_columns([table_a_pk])),
                pk.in_(dups.selectable.with_only_columns([table_b_pk]))
            )
        )

    # Convert query to DataFrame.
    df = io.db_to_df(rows)
    return df


def geom_overlapping(table, key_name, output_table_name):
    """
    Export overlapping geometries from a table into another table.

    The exported table contains the following columns:
        key_name_a, key_name_b: identifiers of the overlapping pair
        relation: DE-9IM representation of their spatial relation
        geom_a, geom_b: corresponding geometries
        overlap: 2D overlapping region (polygons)

    Parameters
    ----------
    table : sqlalchemy.ext.declarative.DeclarativeMeta
        Table ORM class to query for overlapping geometries.
    key_name : str
        Name of column in the queried table containing a unique identifier,
        such as a primary key, to use for cross join and to identify
        geometries in the exported table.
    output_table_name : str
        Name of exported table. Table is created in the same schema as
        the queried table.

    Returns
    -------
    None

    """
    # Create table aliases to cross join table to self.
    table_a = aliased(table)
    table_b = aliased(table)
    table_a_key = getattr(table_a, key_name).label(key_name + '_a')
    table_b_key = getattr(table_b, key_name).label(key_name + '_b')

    # Query for overlaps.
    with db.session() as sess:
        q = sess.query(
            table_a_key, table_b_key,
            func.ST_Relate(table_a.geom, table_b.geom).label('relation'),
            table_a.geom.label('geom_a'), table_b.geom.label('geom_b'),
            # Extract only polygon geometries from intersection.
            func.ST_CollectionExtract(
                func.ST_Intersection(table_a.geom, table_b.geom),
                3
            ).label('overlap')
        ).filter(
            # Use "<" instead of "!=" to prevent duplicates and save time.
            table_a_key < table_b_key,
            func.ST_Intersects(table_a.geom, table_b.geom),
            # Polygon interiors must not intersect.
            ~func.ST_Relate(table_a.geom, table_b.geom, 'FF*F*****')
            # Alternatively, can use ST_Overlaps, ST_Contains, and ST_Within
            # to check for overlap instead of ST_Relate, but this was
            # slightly slower in my testing.
            # or_(
            #     table_a.geom.ST_Overlaps(table_b.geom),
            #     table_a.geom.ST_Contains(table_b.geom),
            #     table_a.geom.ST_Within(table_b.geom)
            # )
        )

    # Create new table from query. This table does not contain constraints,
    # such as primary keys.
    schema = getattr(db.tables, table.__table__.schema)
    io.db_to_db(q, output_table_name, schema)


def geom_unfilled(table, output_table_name):
    """
    Export rows containing interior rings into another table.

    Include the unfilled geometry in the exported table as a new column
    named "unfilled".

    Parameters
    ----------
     table : sqlalchemy.ext.declarative.DeclarativeMeta
        Table ORM class to query for rows containing geometries with
        interior rings.
    output_table_name : str
        Name of exported table. Table is created in the same schema as
        the queried table.

    Returns
    -------
    None

    """
    # Query rows containing geometries with interior rings.
    # Add column for unfilled geometry (outer polygon - polygon).
    # TODO: Ignore unfilled areas that are overlapped by another row.
    with db.session() as sess:
        q = sess.query(
            table,
            func.ST_Difference(
                func.ST_MakePolygon(
                    func.ST_ExteriorRing(
                        func.ST_Dump(table.geom).geom
                    )
                ),
                table.geom
            ).label('unfilled')
        ).filter(
            func.ST_NRings(table.geom) > 1,
        )

    # Create new table from query. This table does not contain constraints,
    # such as primary keys.
    schema = getattr(db.tables, table.__table__.schema)
    io.db_to_db(q, output_table_name, schema)


def reproject(srid, table=None, column=None):
    """
    Reproject table into the specified SRID.

    Either a table or a column must be specified. If a table is specified,
    the geom column will be reprojected.

    Parameters
    ----------
    srid : int
        Spatial Reference System Identifier (SRID).
    table : sqlalchemy.ext.declarative.DeclarativeMeta, optional
        Table ORM class containing geom column to reproject.
    column : sqlalchemy.orm.attributes.InstrumentedAttribute, optional
        Column ORM object to reproject.

    Returns
    -------
    None

    """
    # Get Table and Column objects.
    if column:
        geom = column.property.columns[0]
        t = geom.table
    else:
        t = table.__table__
        geom = t.c.geom

    # Reproject using ST_Transform if column SRID differs from project SRID.
    if srid != geom.type.srid:
        with db.cursor() as cur:
            cur.execute("""
                ALTER TABLE {schema}.{table}
                ALTER COLUMN {g_name} TYPE geometry({g_type}, {srid})
                USING ST_Transform({g_name}, {srid});
            """.format(
                schema=t.schema, table=t.name,
                g_name=geom.name, g_type=geom.type.geometry_type,
                srid=srid))
    else:
        logger.warn("Table {table} already in SRID {srid}".format(
            table=t.name, srid=srid))

    # Refresh ORM.
    db.refresh()


def validate(table=None, column=None):
    """
    Attempt to fix invalid geometries.

    Either a table or a column must be specified. If a table is specified,
    the geom column will be validated.

    Parameters
    ----------
    table : sqlalchemy.ext.declarative.DeclarativeMeta, optional
        Table ORM class containing geom column to validate.
    column : sqlalchemy.orm.attributes.InstrumentedAttribute, optional
        Column ORM object to validate.

    Returns
    -------
    None

    """
    # Get Table and Column objects.
    if column:
        geom = column.property.columns[0]
        t = geom.table
    else:
        t = table.__table__
        geom = t.c.geom

    # Get column data and geometry type.
    data_type = geom.type
    geom_type = data_type.geometry_type
    if "point" in geom_type.lower():
        geom_type_num = 1
    elif "linestring" in geom_type.lower():
        geom_type_num = 2
    elif "polygon" in geom_type.lower():
        geom_type_num = 3
    else:
        geom_type_num = None

    with db.session() as sess:
        # Fix geometries using ST_MakeValid. If geometry type is
        # point/linestring/polygon, only extract elements of those types,
        # to prevent invalid data type errors.
        if geom_type_num:
            valid_geom = func.ST_CollectionExtract(
                func.ST_MakeValid(geom),
                geom_type_num
            )
        else:
            valid_geom = func.ST_MakeValid(geom)
        sess.query(t).filter(
            ~geom.ST_IsValid()
        ).update(
            {geom: valid_geom}, synchronize_session=False
        )


def conform_srids(srid, schema=None, fix=False):
    """
    Reproject all non-conforming geometry columns into the specified SRID.

    Parameters
    ----------
    srid : int
        Spatial Reference System Identifier (SRID).
    schema : schema class, optional
        If schema is specified, only SRIDs within the specified schema
        are conformed.
    fix : bool, optional
        Whether to report and attempt to fix invalid geometries.

    Returns
    -------
    None

    """
    # Iterate over all columns. Reproject geometry columns with SRIDs
    # that differ from project SRID.
    for schema_name, schema_obj in db.tables.__dict__.items():
        if not schema_name.startswith('_'):
            if not schema or schema_name == schema.__name__:
                for table_name, table in schema_obj.__dict__.items():
                    if not table_name.startswith('_'):
                        for c in table.__table__.columns:
                            if isinstance(c.type, Geometry):
                                # Fix geometry if asked to do so.
                                if fix:
                                    if c.name == 'geom':
                                        invalid_df = geom_invalid(table)
                                        if not invalid_df.empty:
                                            logger.warn(invalid_df)
                                            validate(table=table)
                                    else:
                                        validate(column=getattr(table,
                                                                c.name))

                                # Reproject if SRID differs from project SRID.
                                current_srid = c.type.srid
                                if srid != current_srid:
                                    column = getattr(table, c.name)
                                    reproject(srid, table, column)
