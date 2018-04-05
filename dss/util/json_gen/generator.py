import random
from typing import Union, List, Optional

import rstr
from faker import Faker
from faker.providers.python import Provider as PythonProvider
from jsonschema import RefResolver, Draft4Validator
from math import ceil, floor


class JsonProvider(PythonProvider):

    _SUPPORTED_JSON_TYPES = {'str', 'float', 'int', 'iso8601', 'uri', 'email', 'bool'}
    # The more times a value appears in the tuple the greater the probability of it being generated.
    _DEFAULT_VALUE_TYPES = ('str', 'str', 'str', 'str', 'float', 'float', 'int', 'int', 'iso8601', 'uri', 'email',
                            'bool')

    def jsondict(self, nb_elements=10, variable_nb_elements=True, *value_types):
        value_types = self._check_value_types(value_types)
        return self.pydict(nb_elements, variable_nb_elements, *value_types)

    def jsonlist(self, nb_elements=10, variable_nb_elements=True, *value_types):
        value_types = self._check_value_types(value_types)
        return self.pylist(nb_elements, variable_nb_elements, *value_types)

    def _check_value_types(self, value_types) -> tuple:
        if not value_types:
            value_types = self._DEFAULT_VALUE_TYPES
        else:
            assert self._SUPPORTED_JSON_TYPES.issuperset(set(value_types)), "Unsupported types in value_types"
        return value_types


class JsonGenerator(object):
    """Generate a random JSON document based on the provided schema."""

    UNBOUND_MIN_ITEMS = 1
    UNBOUND_MAX_ITEMS = 16

    UNBOUND_MIN_INT = -32000
    UNBOUND_MAX_INT = 32000

    UNBOUND_MIN_STRING = 1
    UNBOUND_MAX_STRING = 128

    UNBOUND_MIN_OBJECTS = 1
    UNBOUND_MAX_OBJECTS = 16

    KEY_LEN = 64

    #  Providers of the Faker library used to generate data in a specific format. The dictionary key name must match
    #  the format field of the JSON schema. Example:
    #   "stop_time": {
    #       "type": "string",
    #       "format": "date-time"
    #   }
    #  The dictionary item must match a valid Faker attribute.
    _default_format_generators = {
        'date-time': 'iso8601',
        'date': 'date',
        'time': 'time',
        'email': 'email'
    }

    def __init__(self, resolver: RefResolver=None, formats: dict=None) -> None:
        """
        :param resolver: used to resolved '$ref' within the schema.
        :param formats: replaces _default_format_generators for determining the type of strings to generate. Must be a
        dict with keys associated with JSON string formats, and items as strings matching a Faker providers.
        Attributes of the Faker library used to generate data in a specific format.
        """
        self.faker = Faker()
        self.faker.add_provider(JsonProvider)
        self._fake_pytypes = [self.faker.jsondict, self.faker.pybool, self.faker.pystr, self.faker.pyint,
                              self.faker.pyfloat, self.faker.jsonlist]
        self.formats = formats if formats else self._default_format_generators
        for value in self.formats.values():
            if not getattr(self.faker, value):
                raise KeyError(f"'{value}' provider not an attribute of Faker.")
        self.resolver = resolver if resolver else RefResolver('', '')
        self.path = []  # type: List[str]

    def generate_json(self, schema: dict) -> dict:
        """
        :param schema: the JSON schema to generate data from.
        :return: generated JSON data
        """
        validator = Draft4Validator(schema, resolver=self.resolver)
        validator.check_schema(schema)
        impostor = self._gen_json(schema)
        validator.validate(impostor)
        return impostor

    def _gen_json(self, schema: dict):
        scope = schema.get("id")
        if scope:
            self.resolver.push_scope(scope)
        try:
            # should be inside an object now otherwise there should be a ref.
            ref = schema.get(u"$ref")
            json_type = schema.get(u"type", "object")
            if ref is not None:
                impostor = self._ref(ref)
            else:
                impostor = getattr(self, f"_{json_type}")(schema)
        finally:
            if scope:
                self.resolver.pop_scope()
        return impostor

    def _ref(self, r: str):
        resolve = getattr(self.resolver, "resolve", None)
        if resolve is None:
            with self.resolver.resolving(r) as resolved:
                impostor = self._gen_json(resolved)
        else:
            scope, resolved = self.resolver.resolve(r)
            self.resolver.push_scope(scope)
            try:
                impostor = self._gen_json(resolved)
            finally:
                self.resolver.pop_scope()
        return impostor

    def _common(self, schema: dict):
        fake = schema.get('fake')
        impostor = schema.get('const')
        enums = schema.get('enum')
        if fake:
            impostor = getattr(self.faker, fake)()
        elif not impostor and enums:
            impostor = random.choice(enums)
        return impostor

    def _object(self, schema: dict) -> dict:
        impostor = self._common(schema)
        if impostor is None:
            required = schema.get('required', [])
            properties = schema.get('properties')
            if properties:
                properties = list(schema['properties'].keys())
            impostor = {}
            # create required
            for j_object in required:
                self.path.append(j_object)
                impostor[j_object] = self._gen_json(schema['properties'][j_object])
                properties.remove(j_object)
                self.path.pop()
            maximum = schema.get('maxProperties', self.UNBOUND_MAX_OBJECTS)
            minimum = schema.get('minProperties', self.UNBOUND_MIN_OBJECTS)
            make_properties = minimum if minimum == maximum else random.randrange(minimum, maximum)
            if len(impostor) < make_properties:
                options = []
                if properties:
                    options.append('pr')
                pattern_properties = schema.get('patternProperties')
                if pattern_properties:
                    options.append('pa')
                    patterns = [pattern for pattern in pattern_properties.keys()]
                additional_properties = schema.get('additionalProperties')
                if additional_properties:
                    options.append('ad')
                while len(impostor) < make_properties:
                    #  make a property if there are properties to make
                    if options:
                        choice = random.choice(options)
                    else:
                        break  # in case there are no other options

                    if choice == 'pr':  # make properties if any remain
                        j_object = random.choice(properties)
                        properties.remove(j_object)
                        impostor[j_object] = self._gen_json(schema['properties'][j_object])
                        if not properties:
                            options.remove('pr')
                    elif choice == 'pa':  # make a patternProperty
                        pattern = str(random.choice(patterns))
                        j_object = rstr.xeger(pattern)[:self.KEY_LEN]
                        impostor[j_object] = self._gen_json(pattern_properties[pattern])
                    elif choice == 'ad':  # make an additionalProperty
                        j_object = self.faker.uuid4()
                        impostor[j_object] = random.choice(self._fake_pytypes)()
        return impostor

    def _number(self, schema: dict) -> Union[int, float]:
        impostor = self._common(schema)
        if impostor is None:
            maximum = schema.get('maximum', schema.get('exclusiveMaximum', self.UNBOUND_MAX_INT) - 1e-12)
            minimum = schema.get('minimum', schema.get('exclusiveMinimum', self.UNBOUND_MIN_INT) + 1e-12)
            if minimum == maximum:
                impostor = minimum
            else:
                multiple_of = schema.get('multipleOf')
                if multiple_of is not None:
                    if multiple_of <= 0:
                        raise ValueError("multipleOf must be > 0")
                    v = self.faker.random_int(ceil(minimum / multiple_of), floor(maximum / multiple_of))
                    impostor = round(v * multiple_of, 12)
                else:
                    impostor = self.faker.random.uniform(minimum, maximum)
        return impostor

    def _integer(self, schema: dict) -> int:
        impostor = self._common(schema)
        if impostor is None:
            maximum = schema.get('maximum', schema.get('exclusiveMaximum', self.UNBOUND_MAX_INT) - 1)
            minimum = schema.get('minimum', schema.get('exclusiveMinimum', self.UNBOUND_MIN_INT) + 1)
            if minimum == maximum:
                impostor = minimum
            else:
                multiple_of = schema.get('multipleOf')
                if multiple_of is not None:
                    if multiple_of <= 0:
                        raise ValueError("multipleOf must be > 0")
                    v = self.faker.random_int(ceil(minimum / multiple_of), floor(maximum / multiple_of))
                    impostor = v * multiple_of
                else:
                    impostor = self.faker.random_int(minimum, maximum)
        return impostor

    def _string(self, schema: dict) -> str:
        impostor = self._common(schema)
        if impostor is None:
            generate_format = schema.get('format')
            generate_pattern = schema.get('pattern')
            if generate_format:
                impostor = getattr(self.faker, self.formats[generate_format])()
            elif generate_pattern:
                impostor = rstr.xeger(generate_pattern)
            else:
                maximum = schema.get('maxLength', self.UNBOUND_MAX_STRING)
                minimum = schema.get('minLength', self.UNBOUND_MIN_STRING)
                impostor = self.faker.pystr(minimum, maximum)
        return impostor

    def _boolean(self, schema: dict) -> bool:
        impostor = self._common(schema)
        if impostor is None:
            impostor = self.faker.pybool()
        return impostor

    def _array(self, schema: dict) -> list:
        items = schema.get('items')
        contains = schema.get('contains')
        unique = schema.get('uniqueItems', False)
        minimum = schema.get('minItems', self.UNBOUND_MIN_ITEMS)
        maximum = schema.get('maxItems', minimum + self.UNBOUND_MAX_ITEMS)
        length = minimum if minimum == maximum else random.randrange(minimum, maximum)

        def simple_gen(make_item):
            if enums:
                impostor.extend(random.choices(enums, k=length))
            else:
                while len(impostor) < length:
                    impostor.append(self._gen_json(make_item))

        def unique_gen(make_item):
            if enums:
                random.shuffle(enums)
                impostor.extend(enums[:length])
            else:
                _retry = 3   # To prevent infinite loops
                while len(impostor) < length and _retry:
                    _retry -= 1
                    u_item = self._gen_json(make_item)
                    if u_item not in impostor:
                        impostor.append(u_item)
                        _retry = 3

        impostor = schema.get('const', [])
        enums = schema.get('enum')
        if isinstance(items, dict):
            if contains:
                impostor.append(self._gen_json(contains))
            if unique:
                unique_gen(items)
            else:
                simple_gen(items)
        elif isinstance(items, list):
            # TODO: Handle contains
            additional_items = schema.get('additionalItems')
            if unique:
                item_count = len(items)
                i = 0
                retry = 0  # To prevent infinite loops
                while i < item_count and retry < 3:
                    item = self._gen_json(items[i])
                    retry += 1
                    if item not in impostor:
                        impostor.append(item)
                        i += 1
                        retry = 0
                if additional_items:
                    unique_gen(additional_items)
            else:
                for item in items:
                    impostor.append(self._gen_json(item))
                if additional_items:
                    simple_gen(additional_items)
        return impostor
