import os

import pandas as pd

from .io import exec_sql
from .utils import DataLoader


"""
Functions in this module are deprecated and no longer tested.

Loading data is now handled by DataLoader methods.

"""


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
            print("Loading %s." % shp_name)
            path = path_func(shp_name, shp_dict[shp_name][0])
            loader.load_shp(filename=path,
                            table=shape_category + '_' + shp_name,
                            srid=shp_dict[shp_name][1], drop=True)


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
    dtypes[dtypes == 'float64'] = 'float'
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


def load_multiple_delimited_files(files, config_filename=None):
    """
     Load multiple delimited text files to Postgres according to a given dictionary
    of file information.

    Parameters
    ----------
    files : dict
        Dictionary of dictionaries where the top-level key is file category,
        which also corresponds to the name of the directory within the data_dir
        containing this category of files. The sub-dictionaries are
        dictionaries where the keys correspond to the geography name and the
        value is a tuple of the form (file_name, table_name, delimiter).  If SRID is
        None, then default config SRID is used.

        Example dictionary
             {'parcels' :  ##Looks for 'parcels' directory within the data_dir
                  ##Looks for 'marin' directory within parcels dir
                  {'alameda':('alameda_parcel_info.txt', 'alameda_pcl_info', '\t'),
                  'napa':('napa_parcel_info.csv', 'napa_pcl_info', ','),
                  }
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
        Loads delimited files to the database (returns nothing)

    """
    def subpath(base_dir):
        def func(shp_table_name, shp_path):
            input_dir = base_dir
            return os.path.join(DataLoader().directory, input_dir, shp_table_name, shp_path)
        return func
    for category in files:
        path_func = subpath(category)
        del_dict = files[category]
        for name in del_dict:
            path = path_func(name, del_dict[name][0])
            table_name = del_dict[name][1]
            delimiter = del_dict[name][2]
            print('Loading %s as %s' % (del_dict[name][0], table_name))
            load_delimited_file(path, table_name, delimiter=delimiter)
