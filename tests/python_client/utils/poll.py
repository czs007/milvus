"""Polling intervals that start dense and back off, instead of a fixed sleep per round.

Why: on a full nightly run (11736 cases, 60514s of accumulated machine time) the
bulk-insert family averages ~32s per case, while the server side finishes the import
in 0.2s median / 0.6s P90 (293 tasks, 77s in total) on datasets of 60 rows. The time
is not spent in the server -- it is spent sleeping for a fixed interval before looking
again.

Semantics are unchanged: the same condition is polled under the same timeout, only the
first few rounds ask more often. `cap` bounds both the starting value and the steady
state, so passing the interval a loop used before guarantees no round is ever slower
than it used to be.
"""

DEFAULT_STEPS = (0.1, 0.2, 0.5, 1.0, 2.0)


def poll_intervals(cap=2.0, steps=DEFAULT_STEPS):
    """Yield polling intervals, repeating the last one forever.

    >>> list(itertools.islice(poll_intervals(1.0), 6))
    [0.1, 0.2, 0.5, 1.0, 1.0, 1.0]
    """
    seq = tuple(s for s in steps if s <= cap) or (cap,)
    for s in seq:
        yield s
    while True:
        yield seq[-1]
