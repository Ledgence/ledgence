"""Shipping cost in cents for a nonnegative integer order total."""


def shipping_cost(total_cents):
    if type(total_cents) is not int:
        raise TypeError("total_cents must be an integer")
    if total_cents < 0:
        raise ValueError("total_cents must be nonnegative")
    return 0 if total_cents > 10000 else 500
