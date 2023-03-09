# Pydian - pythonic data interchange

Pydian is a pure Python library for repeatable and sharable data mappings and transformations. Pydian reduces boilerplate and provides a framework for expressively mapping data through Python `dict` objects and native sequence comprehensions.

With Pydian, you can grab and manipulate data from heavily nested dicts using `get`:
```python
from pydian import get

# Some arbitrary source dict
payload = {
    'some': {
        'deeply': {
            'nested': [{
                'value': 'here!'
            }]
        }
    },
    'list_of_objects': [
        {'val': 1},
        {'val': 2},
        {'val': 3}
    ]
}

# Conveniently get values and chain operations
assert get(payload, 'some.deeply.nested[0].value', apply=str.upper) == 'HERE!'

# Unwrap list structures with [*]
assert get(payload, 'list_of_objects[*].val') == [1,2,3]

# Safely specify your logic with built-in null checking (handle `None` instead of a stack trace!)
assert get(payload, 'somekey.nope.not.there', apply=str.upper) == None
```

For complex data mapping tasks, the `Mapper` framework provides additional benefits:
```python
from pydian import Mapper, get

# Same example from above
payload = { 
    'some': { 'deeply': { 'nested': [{ 'value': 'here!' }] } },
    'list_of_objects': [{'val': 1}, {'val': 2}, {'val': 3}]
}

# Specify your logic as data in a centralized mapping function (dict -> dict)
mapping_fn = lambda source: {
    'static_value': 'Some static data',
    'uppercase_nested_val': get(source, 'some.deeply.nested[0].value', apply=str.upper),
    'unwrapped_list': get(source, 'list_of_objects[*].val'),
    "optional_value": get(source, "somekey.nope.not.there")
}

# Use the `Mapper` class to get post-processing features like null value removal and conditional dropping
mapper = Mapper(mapping_fn)

# Get an iterpretable result that syntactically matches the mapping function!
assert mapper(payload) == {
    'static_value': 'Some static data',
    'uppercase_nested_val': 'HERE!',
    'unwrapped_list': [1, 2, 3]
    # Empty values like `None` are removed from the result by the `Mapper` class
}
```

We can do more advanced things like conditional dropping, e.g. continuing the example above:

```python
# Same example from above
from pydian import DROP, Mapper, get

# Same example from above
payload = { 
    'some': { 'deeply': { 'nested': [{ 'value': 'here!' }] } },
    'list_of_objects': [{'val': 1}, {'val': 2}, {'val': 3}]
}

mapping_fn_drop_example = lambda source: {
    # Check for keys that may or may not be present (without causing a stack trace)
    "optional_value": get(source, "somekey.nope.not.there"),
    # Safely apply values only when data is present
    'optional_value_with_apply': get(source, 'somekey.nope.not.there', apply=str.upper),
    # Use the DROP enum to set objects to `None` relative to the data element
    'maybe_present_object': {
        'other_static_value': 'More static data', # Without DROP, static data is stays present
        'maybe_present_value': get(source, 'somekey.nope.not.there', apply=str.upper, drop_level=DROP.THIS_OBJECT)
    },
}

# The `Mapper` class processes the DROP values
mapper_drop_example = Mapper(mapping_fn_drop_example)

# Note that failed operations were removed instead of throwing an error.
# This is useful for optional data fields which data standards can have a lot of!
assert mapper_drop_example(payload) == {
    # Empty values like `None` are removed from the result by the `Mapper` class
}
```

See the [mapping test examples](./tests/test_dicts.py) for a more involved look at some of the features + intended use-cases.

## `get` Functionality

Pydian defines a special `get` function that leverages [JMESPath](https://jmespath.org/) and provides a simple syntax for:
- Getting nested items using JMESPath syntax (examples [here](https://jmespath.org/examples.html))
- Chaining successful operations with `apply`
- Add a pre-condition with `only_if`
- Specifying conditional dropping with `drop_level` (see [below](./README.md#conditional-dropping))
- Flattening resulting lists-of-lists with `flatten`

`None` handling is built-in which reduces boilerplate code!

## `Mapper` Functionality

The `Mapper` framework provides a consistent way of abstracting mapping steps as well as several useful post-processing steps, including:
1. [Null value removal](./README.md#null-value-removal): Removing `None`, `""`, `[]`, `{}` values from the final result
2. [Conditional dropping](./README.md#conditional-dropping): Drop key(s)/object(s) if a specific value is `None`

### Null value removal

This is just a parameter on the `Mapper` object (`remove_empty`) which defaults to `True`.

An "empty" value is defined in [lib/util.py](./pydian/lib/util.py) and includes: `None`, `""`, `[]`, `{}`

### Conditional dropping

This can be done during value evaluation in `get` which the `Mapper` object cleans up at runtime:
```python
from pydian import DROP, Mapper, get

payload = { 'a': 'b' }

mapping_fn = lambda source: {
    'object': {
        # The static element is always present, but what if we want to conditionally remove it?
        'static_value': 'Some value',
        # ... use the DROP enum and the Mapper framework cleans it up!
        'maybe_present_value?': get(source, 'some_missing_key', drop_level=DROP.THIS_OBJECT),
    }
}

# Use a Mapper to handle the DROP enum. During handling, correpsonding values are set to `None`
mapper = Mapper(mapping_fn, remove_empty=False)

assert mapper(payload) == {
    'object': None # as opposed to `{'static_value': 'Some value'}`
}
```

## `pydian.partials` Library

For chained operations, it's pretty common to write a bunch of `lambda` functions. While this works, writing these can get verbose and cumbersome (e.g. writing something like `lambda x: x == 1` to check if something equals 1).

As a convenience, Pydian provides library of partial wrappers that quickly provide 1-input, 1-output functions:
```python
from pydian import get
import pydian.partials as p

# Arbitrary example
source = {
    'some_values': [
        250,
        350,
        450
    ]
}

assert p.equals(1)(1) == True
assert p.equivalent(False)(False) == True
assert get(source, 'some_values', apply=p.index(0), only_if=p.contains(350)) == 250
assert get(source, 'some_values', apply=p.index(0), only_if=p.contains(9000)) == None
assert get(source, 'some_values', apply=p.index(1)) == 350
assert get(source, 'some_values', apply=p.keep(2)) == [250, 350]
```

## Issues

Please submit a GitHub Issue for any bugs + feature requests and we'll take a look!
