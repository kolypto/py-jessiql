""" Execute a Query Object: everything needed to execute it

Overview:

* Query is the high-level interface to QueryObject execution
* QueryExecutor is the low-level interface.
  But to be honest, the difference is minimal :) It just lacks some non-essential features.
"""

from .query import Query
