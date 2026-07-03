"""Machine learning layer for the Customer Intelligence Platform.

The ML layer reads its features exclusively from ``marts.mart_customer_360``
(the per-customer feature store) and writes scores to the ``ml`` schema. This
package is split into three sub-packages following the design's ML module
structure:

* :mod:`ml.features` — feature loading (DuckDB) and transformation utilities.
* ``ml.models``       — individual model train/score/register modules (Task 26+).
* ``ml.scoring``      — orchestration that runs every model for a run date.

Only :mod:`ml.features` is implemented at this point (Task 25). No model
training code lives here yet.
"""

from __future__ import annotations
