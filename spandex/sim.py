import logging

from decorator import decorator
from urbansim.sim import simulation


# Set up logging system.
logging.basicConfig()
logger = logging.getLogger(__name__)


# Import from PyGraphviz if available.
try:
    from pygraphviz import AGraph
except ImportError:
    # PyGraphviz currently does not support Python 3.
    logger.warn("PyGraphviz not available for plotting graphs.")


def column(table_name, fillna=None, astype=None, groupby=None, agg=None):
    def decorated(f):
        def wrapped(f, *args, **kwargs):
            out = f(*args, **kwargs)
            if fillna:
                out.fillna(fillna, inplace=True)
            if astype:
                out = out.astype(astype, copy=False)
            if agg:
                out = getattr(out.groupby(groupby), agg)()
            return out
        simulation.add_column(table_name, f.__name__, decorator(wrapped, f))
    return decorated


def plot(table_names=None):
    """
    Plot relationships between columns and tables using Graphviz.

    Parameters
    ----------
    table_names : iterable of str, optional
        Names of UrbanSim registered tables to plot.
        Defaults to all registered tables.

    Returns
    -------
    graph : pygraphviz.AGraph
        PyGraphviz graph object.

    """
    if not table_names:
        # Default to all registered tables.
        table_names = simulation.list_tables()

    graph = AGraph(directed=True)
    graph.graph_attr['fontname'] = 'Sans'
    graph.graph_attr['fontsize'] = 28
    graph.node_attr['shape'] = 'box'
    graph.node_attr['fontname'] = 'Sans'
    graph.node_attr['fontcolor'] = 'blue'
    graph.edge_attr['weight'] = 2

    # Add each registered table as a subgraph with columns as nodes.
    for table_name in table_names:
        subgraph = graph.add_subgraph(name='cluster_' + table_name,
                                      label=table_name, fontcolor='red')
        table = simulation.get_table(table_name)
        for column_name in table.columns:
            full_name = table_name + '.' + column_name
            subgraph.add_node(full_name, label=column_name)

    # Iterate over computed columns to build nodes.
    for key, wrapped_col in simulation._COLUMNS.items():
        table_name = key[0]
        column_name = key[1]

        # Combine inputs from argument names and argument default values.
        args = list(wrapped_col._argspec.args)
        if wrapped_col._argspec.defaults:
            default_args = list(wrapped_col._argspec.defaults)
        else:
            default_args = []
        inputs = args[:len(args) - len(default_args)] + default_args

        # Create edge from each input column to the computed column.
        for input_name in inputs:
            full_name = table_name + '.' + column_name
            graph.add_edge(input_name, full_name)

    graph.layout(prog='dot')
    return graph
