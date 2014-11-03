from decorator import decorator
from urbansim.sim import simulation


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
