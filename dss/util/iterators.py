from itertools import tee
from typing import Iterator, TypeVar, Optional, Tuple, Iterable, NamedTuple

T = TypeVar('T')
OT = Optional[T]
Column = Iterator[T]


class zipalign(Iterator['zipalign.Row']):
    """
    An iterator that detects differences between any number of input iterators as long as the input iterators yield
    their elements in strictly ascending order. The signature is similar to that of itertools.zip_longest(*).

    For example, given three tuples …

    >>> columns = [(1, 2),(0, 2),(0, 2, 3)]

    … (written vertically in columnar form) zipalign yields the representation on the right:

    ┌───┬───┬───┐   Align       ┌───┬───┐    Prepend    ┌───┬───┬───┬───┐
    │ 1 │ 0 │ 0 │    by         │ 0 │ 0 │  column with  │ 0 │ 1 │ 0 │ 0 │
    ├───┼───┼───┤   value   ┌───┼───┴───┘    minimum    ├───┼───┼───┼───┤    (Empty
    │ 2 │ 2 │ 2 │ ────────▶ │ 1 │         ────────────▶ │ 1 │ 1 │ 2 │ 2 │    cells
    └───┴───┼───┤           ├───┼───┬───┐  Values not   ├───┼───┼───┼───┤    denote
            │ 3 │           │ 2 │ 2 │ 2 │   equal to    │ 2 │ 2 │ 2 │ 2 │    None)
            └───┘           └───┴───┼───┤    minimum    ├───┼───┴───┼───┤
                                    │ 3 │   are to be   │ 3 │       │ 3 │
                                    └───┘    ignored    └───┘       └───┘

    While the center representation is the most intuitive one, zipalign returns the one on the right as it allows for
    the elimination of intrinsic state and more efficient execution. It is also richer in that it distinguishes
    between cells missing from an iterator (value != min) and cells past the end of an iterator (value is None).

    >>> i = zipalign(columns)
    >>> next(i)
    Row(min=0, values=(1, 0, 0))
    >>> next(i)
    Row(min=1, values=(1, 2, 2))
    >>> next(i)
    Row(min=2, values=(2, 2, 2))
    >>> next(i)
    Row(min=3, values=(None, None, 3))
    >>> next(i)
    Traceback (most recent call last):
    ...
    StopIteration

    Converting the actual result back to the more intuitive representation is straight-forward …

    >>> [tuple(val if val == row.min else None for val in row.values) for row in zipalign(columns)]
    [(None, 0, 0), (1, None, None), (2, 2, 2), (None, None, 3)]

    … and row objects offer a convenience method for that:

    >>> [row.norm() for row in zipalign(columns)]
    [(None, 0, 0), (1, None, None), (2, 2, 2), (None, None, 3)]

    A few more properties: For one, exhaustion is permanent:

    >>> next(i)
    Traceback (most recent call last):
    ...
    StopIteration

    If any input iterators violate the ordering constraint, the behavior is well-defined: an exception is raised as
    soon as the out-of-order element is detected.

    >>> list(zipalign([(1, 0)]))
    Traceback (most recent call last):
    ...
    ValueError: Input iterator yielded value out of order

    Given a Row object, a zipalign iterator can resume where another zipalign iterator left off:

    >>> columns = list(map(iter, columns))
    >>> row = next(zipalign(columns)); row
    Row(min=0, values=(1, 0, 0))
    >>> next(zipalign(columns, row=row))
    Row(min=1, values=(1, 2, 2))

    Resumption also works with a plain iterable instead of a Row instance …

    >>> next(zipalign(columns, row=[1, 2, 2]))
    Row(min=2, values=(2, 2, 2))

    … provided that its length is equal to the number of columns.

    >>> next(zipalign(columns, row=[1, 2, 2, 3]))
    Traceback (most recent call last):
    ValueError: Row length (4) does not match # of columns (3)

    We can resume at any stage …

    >>> next(zipalign(columns, row=[2, 2, 2]))
    Row(min=3, values=(None, None, 3))

    … even at the last row.

    >>> next(zipalign(columns, row=[None, None, 3]))
    Traceback (most recent call last):
    ...
    StopIteration
    """

    class Row(NamedTuple):
        """
        The type of the elements yielded by zipalign. It consists of a tuple of values (the row) and their minimum.

        >>> Row = zipalign.Row
        >>> row = Row(min=1, values=(1, 2, 3))

        While a Row is a typing.NamedTuple consisting of the `min` and `values` elements, its length and iterator are
        those of the `values` element:

        >>> len(row)
        3
        >>> list(row)
        [1, 2, 3]

        This comes in handy when converting Row instances to JSON and back:

        >>> import json
        >>> row == Row.from_values(json.loads(json.dumps(row)))
        True
        """
        min: OT
        values: Tuple[OT, ...]

        @classmethod
        def from_values(cls, values: Iterable[OT]):
            v1, v2 = tee(values)
            # noinspection PyArgumentList
            return cls(min=min((val for val in v1 if val is not None), default=None), values=tuple(v2))

        def _fetch(self, val: OT, column: Column) -> OT:
            if val == self.min:
                try:
                    val = next(column)
                except StopIteration:
                    val = None
                else:
                    if not (self.min is None or self.min < val):
                        raise ValueError(f"Input iterator yielded value out of order")
            return val

        def next(self, columns: Tuple[Column, ...]):
            return self.from_values(map(self._fetch, self.values, columns))

        def norm(self) -> Tuple[OT, ...]:
            return tuple(val if val == self.min else None for val in self.values)

        def __iter__(self):
            return iter(self.values)

        def __len__(self) -> int:
            return len(self.values)

    def __init__(self, columns: Iterable[Column], row: Optional[Iterable[OT]] = None) -> None:
        self.columns = tuple(map(iter, columns))
        if row is None:
            self.row = self.Row.from_values((None,) * len(self.columns))
        else:
            self.row = self.Row.from_values(row)
            if len(self.row) != len(self.columns):
                raise ValueError(f"Row length ({len(self.row)}) does not match # of columns ({len(self.columns)})")

    def __next__(self) -> Row:
        if self.row is None:
            raise StopIteration
        else:
            row = self.row.next(self.columns)
            if all(val is None for val in row.values):
                self.row = None
                raise StopIteration
            else:
                self.row = row
                return row
