""" OkOk helpers for unit-tests: okay, okay! we're equal! """


class _Whatever:
    """ Ok, whatever

    Use to pass equality tests with whatever objects

    Example:
        assert [a, b, c] == [a, Whatever(), Whatever()]
    """
    def __eq__(self, other):
        return True

    def __repr__(self):
        return '<Whatever>'


Whatever = _Whatever()
