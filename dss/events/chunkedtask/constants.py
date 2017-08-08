TIME_OVERHEAD_FACTOR = 2.0
"""
If a unit of work is estimated to take X milliseconds, then we must have at least TIME_OVERHEAD_FACTOR * X milliseconds
of remaining time if we schedule additional work.  Otherwise, we will serialize and schedule the work for the future.
"""
