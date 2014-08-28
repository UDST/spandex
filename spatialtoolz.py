import psycopg2
import pandas as pd, numpy as np
import pandas.io.sql as sql
import urbansim.sim.simulation as sim
from utils import DataLoader

def tag(target_table_name, target_field, source_table_name, source_table_field, how='point_in_poly', target_df=None):
    """
    Tags target table with attribute of another table based on spatial relationship.

    Parameters
    ----------
    target_table_name : str
        Name of table to be tagged.  New field will be added, or existing field updated.
    target_field : str
        Name of field in target_table to add (if doesn't exist) or update (if exists).
    source_table_name : str
        Name of table containing information to tag target_table with.
    source_field : str
        Name of field in source_table that contains the information.
    how : str, optional
        How to relate the two tables spatially.  If not specified, defaults to 'point_in_poly'
    target_df : DataFrame, optional
        DataFrame to update based on the tagging operation.

    Returns
    -------
    None : None
        Field is added or updated on the target_table in the database, and returns nothing.
        Unless target_df argument is used, in which case return value is DataFrame with the
        new/updated column.

    """
    check_srid_equality(target_table_name, source_table_name)
        
    if db_col_exists(target_table_name, target_field) == False:
        add_integer_field(target_table_name, target_field)
        
    if how == 'point_in_poly':
        if db_col_exists(target_table_name, 'centroid') == True:
            exec_sql("update %s set %s = b.%s from %s b where st_within(%s.centroid,b.geom)" % 
                                        (target_table_name, target_field, source_table_field, source_table_name, target_table_name)) 
        else:
            exec_sql("update %s set %s = b.%s from %s b where st_within(ST_centroid(%s.geom),b.geom)" % 
                                        (target_table_name, target_field, source_table_field, source_table_name, target_table_name))
    
    if target_df:
        return update_df(target_df, target_field, target_table_name)
        
        
def proportion_overlap(target_table_name, overlapping_table_name, target_field, target_df=None):
    """
    Calculates proportion overlap between target table's geometry and another table's 
    geometry. Populates field in target table with proportion overlap value.

    Parameters
    ----------
    target_table_name : str
        Name of table being overlapped.  New field will be added, or existing field updated.
    overlapping_table_name : str
        Name of table containing geometry that overlaps with target table's geometry.
    target_field : str
        Name of field in target_table to add (if doesn't exist) or update (if exists). This
        is where proportion overlap value will be stored.
    target_df : DataFrame, optional
        DataFrame to update based on the proportion overlap calculation.

    Returns
    -------
    None : None
        Field is added or updated on the target_table in the database, and returns nothing.
        Unless target_df argument is used, in which case return value is DataFrame with the
        new/updated column.

    """
    check_srid_equality(target_table_name, overlapping_table_name)

    if db_col_exists(target_table_name, target_field) == False:
        add_numeric_field(target_table_name, target_field)
        
    calc_area(target_table_name)
        
    exec_sql("UPDATE %s SET %s = (SELECT SUM(ST_Area(ST_Intersection(%s.geom, %s.geom))) FROM %s WHERE ST_Intersects(%s.geom, %s.geom)) / %s.calc_area;" % (target_table_name, target_field, target_table_name, overlapping_table_name, overlapping_table_name, target_table_name, overlapping_table_name, target_table_name))
    
    if target_df:
        return update_df(target_df, target_field, target_table_name)
    
    
def get_srid(table_name, field):
    """Returns SRID of specified table/field."""
    try:
        return db_to_df("SELECT FIND_SRID('public', '%s', '%s')" % (table_name, field)).values[0][0]
    except:
        Pass
    
    
def srid_equality(target_table_name, source_table_name):
    """Checks if there are multiple projections between two tables."""
    srids = []
    def check_append_srid(table_name, field_name):
        if db_col_exists(table_name, field_name):
            srids.append(get_srid(table_name,field_name))
    check_append_srid(target_table_name, 'geom')
    check_append_srid(source_table_name, 'geom')
    check_append_srid(target_table_name, 'centroid')
    check_append_srid(source_table_name, 'centroid')
    srids = np.unique(srids)
    return False if len(srids[srids>0]) > 1 else True  
    
    
def check_srid_equality(table1, table2):
    if srid_equality(table1, table2) == False:
        raise Exception('Projections are different')
    
    
def calc_area(table_name):
    if db_col_exists(table_name, 'calc_area') == False:
        add_numeric_field(table_name, 'calc_area') 
        exec_sql("UPDATE %s SET calc_area = ST_Area(%s.geom);" % (table_name, table_name))
        
        
def update_df(df, column_name, db_table_name):
    df_idx_name = df.index.name
    new_col = db_to_df("select %s, %s from %s" % (df_idx_name, column_name, db_table_name)).set_index(df_idx_name)[column_name]
    df[column_name] = new_col
    return df
    
    
def db_col_exists(table_name, column_name):
    """Tests if column on database table exists"""
    test = db_to_df("SELECT column_name FROM information_schema.columns WHERE table_name='%s' and column_name='%s';"%(table_name,column_name))
    return True if len(test) > 0 else False

    
def add_integer_field(table_name, field_to_add):
    """Add integer field to table."""
    exec_sql("alter table %s add %s integer default 0;" % (table_name, field_to_add))
    
    
def add_numeric_field(table_name, field_to_add):
    """Add numeric field to table."""
    exec_sql("alter table %s add %s numeric default 0.0;" % (table_name, field_to_add))

    
def exec_sql(query):
    """Executes SQL query."""
    cur = sim.get_injectable('cur')
    conn = sim.get_injectable('conn')
    cur.execute(query)
    conn.commit()
    
    
def db_to_df(query):
    """Executes SQL query and returns DataFrame."""
    conn = sim.get_injectable('conn')
    return sql.read_frame(query, conn)
    
    
def reproject(target_table, config_dir, geometry_column='geom' , new_table=None):
    """
    Reprojects target table into the srid specified in the project config

    Parameters
    ----------
    target_table: str
        Name of table to reproject.  Default is in-place reprojection.
    geometry_column : str
        Name of the geometry column in the target table. Default is 'geom'.
    config_dir : str
        Path to the directory where the project config is stored.
    source_field : str
        Name of field in source_table that contains the information.
    new_table: str, optional
        If new_table is specified, a copy of target table is made with name new_table
    
    Returns
    -------
    None : None
        Target table's geometry column is reprojected to the SRID found in the config file.
        Function detects current target table SRID and project SRID and converts on the database.

    """
    project_srid = str(DataLoader(config_dir).srid)
    table_srid = str(get_srid(target_table, geometry_column))
    if new_table:
        exec_sql("CREATE TABLE %s as SELECT * FROM %s" % (new_table, target_table))
        exec_sql("SELECT UpdateGeometrySRID('%s', '%s', %s)" % (new_table, geometry_column, project_srid))
        exec_sql("UPDATE %s SET %s = ST_TRANSFORM(ST_SetSRID(%s, %s), %s)" % (new_table, geometry_column, geometry_column, table_srid, project_srid))
    else:
        exec_sql("SELECT UpdateGeometrySRID('%s', '%s', %s)" % (target_table, geometry_column, project_srid))
        exec_sql("UPDATE %s SET %s = ST_TRANSFORM(ST_SetSRID(%s, %s), %s)" % (target_table, geometry_column, geometry_column, table_srid, project_srid))
        
        
def conform_srids(config_dir):
    """
    Reprojects all non-conforming geometry columns into project SRID

    Parameters
    ----------
    config_dir : str
        Path to the directory where the project config is stored.
    
    Returns
    -------
    None : None
        Nonconforming tables' geometry columns are reprojected to the SRID found in the config file.

    """
    geoms = db_to_df("select f_table_name, f_geometry_column, srid from geometry_columns;")
    project_srid = DataLoader(config_dir).srid
    geoms = geoms[geoms.srid!=project_srid]
    for item in geoms.index:
        target_table = geoms.f_table_name[geoms.index==item]
        geom_col = geoms.f_geometry_column[geoms.index==item]
        reproject(target_table[item], config_dir, geometry_column=geom_col[item])