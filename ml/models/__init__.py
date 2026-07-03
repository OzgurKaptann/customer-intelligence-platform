"""Individual ML model modules for the Customer Intelligence Platform.

Each module in this package encapsulates the train / score / register lifecycle
of one model, reading its features from ``marts.mart_customer_360`` via
:mod:`ml.features` and logging to MLflow. Modules are independent so new models
can be added to the daily scoring pipeline without touching existing ones
(NFR-2.3, NFR-4.4).

Implemented:

* :mod:`ml.models.segmentation` — RFM k-means customer segmentation (Task 26).
* :mod:`ml.models.ltv` — gradient-boosted 12-month LTV scoring (Task 28).
* :mod:`ml.models.churn` — random-forest churn risk scoring (Task 30).
"""

from __future__ import annotations
