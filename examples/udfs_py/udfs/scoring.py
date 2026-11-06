"""Plain Python: no PyFlink imports, so it can be unit-tested and reused anywhere."""
import math


def churn_risk(days_inactive, orders_90d):
    """Logistic churn score in [0, 1]: rises with inactivity, falls with recent orders."""
    if days_inactive is None:
        return None
    z = 0.08 * days_inactive - 0.6 * (orders_90d or 0) - 2.0
    return round(1 / (1 + math.exp(-z)), 4)
