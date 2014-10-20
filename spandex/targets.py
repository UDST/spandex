"""
Functionality for scaling agent attributes or the number of agents to
match targets.

"""
from __future__ import division, print_function


def scale_col_to_target_mean(col, target):
    """
    Scale a column's values so that their mean matches a target.

    Parameters
    ----------
    col : pandas.Series
    target : float

    Returns
    -------
    scaled : pandas.Series

    """
    current_mean = col.mean()
    multiplier = target / current_mean
    return col * multiplier
