"""Plain Python: no PyFlink imports, so these are unit-testable anywhere. cascade wraps them."""


class WeightedAvg:
    """SUM(value * weight) / SUM(weight). The accumulator is [sum, weight]."""

    def create_accumulator(self):
        return [0.0, 0.0]

    def accumulate(self, acc, value, weight):
        if value is not None and weight is not None:
            acc[0] += value * weight
            acc[1] += weight

    def retract(self, acc, value, weight):
        if value is not None and weight is not None:
            acc[0] -= value * weight
            acc[1] -= weight

    def merge(self, acc, others):
        for o in others:
            acc[0] += o[0]
            acc[1] += o[1]

    def get_value(self, acc):
        return acc[0] / acc[1] if acc[1] else None


def split_tags(tags):
    """One row per comma-separated tag, with its position."""
    if tags is None:
        return
    for i, tag in enumerate(tags.split(",")):
        yield tag.strip(), i
