from collections import OrderedDict, Mapping


class frozendict(Mapping):
    """
    An immutable wrapper around dictionaries that implements the complete :py:class:`collections.Mapping`
    interface. It can be used as a drop-in replacement for dictionaries where immutability is desired.

    >>> fd1 = frozendict(a=1, b=2)

    Frozendicts are hashable and thus can be used in sets and as keys on dictionaries:

    >>> {fd1:1, fd1:2}[fd1]
    2

    They can be copied and have pass-by-value semantics (as opposed to pass-by-reference sematics):

    >>> fd2=fd1.copy()
    >>> fd1 == fd2
    True
    >>> fd1 is fd2
    False
    >>> {fd1:1, fd2:2}[fd1]
    2

    They are also immutable:

    >>> fd1['a'] = 3
    Traceback (most recent call last):
    ...
    TypeError: 'frozendict' object does not support item assignment
    >>> del fd1['a']
    Traceback (most recent call last):
    ...
    TypeError: 'frozendict' object does not support item deletion
    >>> fd1.keys().remove('a')
    Traceback (most recent call last):
    ...
    AttributeError: 'KeysView' object has no attribute 'remove'
    """

    dict_cls = dict

    def __init__(self, *args, **kwargs):
        self._dict = self.dict_cls(*args, **kwargs)
        self._hash = None

    def __getitem__(self, key):
        return self._dict[key]

    def __contains__(self, key):
        return key in self._dict

    def copy(self, **add_or_replace):
        return self.__class__(self, **add_or_replace)

    def __iter__(self):
        return iter(self._dict)

    def __len__(self):
        return len(self._dict)

    def __repr__(self):
        return '<%s %r>' % (self.__class__.__name__, self._dict)

    def __hash__(self):
        if self._hash is None:
            h = 0
            for key, value in self._dict.items():
                h ^= hash((key, value))
            self._hash = h
        return self._hash


class FrozenOrderedDict(frozendict):
    """
    A frozendict subclass that maintains key order
    """

    dict_cls = OrderedDict
