from typing import Union, Mapping, Any, List

JSONObj = Mapping[str, Any]

# Strictly speaking, this is the generic JSON type:

AnyJSON = Union[str, int, float, bool, None, JSONObj, List[Any]]

# Most JSON structures, however, start with an JSON object, so we'll use the shorter name for that type:

JSON = Mapping[str, AnyJSON]
