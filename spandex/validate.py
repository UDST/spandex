import ast
import orca
import yaml

import pandas as pd
import orca_test as ot

from orca_test import OrcaSpec, TableSpec, ColumnSpec


def yaml_to_spec(file_path, table_name):
    """
    Convert yaml specification to an orca_test TableSpec with ColumnSpecs

    Parameters
    ----------
    file_path : str
        Path to the yaml spec file.
    table_name : str
        Name of the table specification within the yaml file.

    Returns
    -------
    tspec : orca_test.TableSpec
        orca_test spec with name = table_name

    """
    spec = yaml.load(open(file_path))
    tspec = TableSpec(table_name)
    for item in spec[table_name].keys():
        args = {}
        for check in spec[table_name][item]:
            arg = dict(e.split('=') for e in check.split(', '))
            args.update(arg)
        args = {k: ast.literal_eval(v) for k, v in args.items()
                if type(v)==str}
        tspec.columns.append(ColumnSpec(item, **args))
    return tspec


def validate_table(data, table_name, file_path):
    """
    Check a csv data table or pandas.DataFrame against a
    schema specified in yaml.

    Parameters
    ----------
    data : str or pandas.DataFrame
        Path to csv file or dataframe to be checked.
    table_name : str
        Name of the table as it appears in the yaml spec.
    file_path : str
        Path to the yaml schema specification file.

    Returns
    -------
    exceptions : list
        List of all exceptions found by orca_test.

    """
    if type(data) == pd.DataFrame:
        pass
    else:
        data = pd.read_csv(data)
    
    orca.add_table(table_name, data)
    
    table_spec = yaml_to_spec(file_path, table_name)
    
    exceptions = []
    for column in table_spec.columns:
        try:
            ot.assert_column_spec(table_name, column)
        except Exception as e:
            exceptions.extend(e)
        finally:
            pass
    print(found {} exceptions in {}).format(len(exceptions), table_name)
    
    return exceptions
