# Pydian - pythonic data interchange

Pydian is a pure Python library for readable and repeatable data mappings. Pydian reduces boilerplate for data manipulation and provides a framework for expressive data wrangling.

Using Pydian, developers can collaboratively and incrementally write data mappings that are expressive, safe, and reusable. Similar to how libraries like React were able to streamline UI components for frontend development, Pydian aims to streamline data transformations for backend development.

## `get` specific data, then do stuff

The key idea behind is the following: `get` data from an object, and if it succeeded, do stuff to it.

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
assert get(payload, 'some.deeply.nested[100].value', apply=str.upper) == None
```

That's it! Additional constructs are added for more complex mapping operations (`Mapper`).

What makes this different from regular operations? Pydian is designed with readibility and reusability in mind:
1. By default, on failure `get` returns `None`. This offers a more flexible alternative to direct indexing (e.g. `array[0]`).
2. For a specific field, you can concisely fit all of your functional logic into _one line_ of Python. This improves readability and maintainability.
3. All functions are "pure" and can be effectively reused and imported without side effects. This encapsulates behavior and promotes reusability.

## Developer-friendly API

If you are working with `dict`s, you can use:
- A [`get`](./pydian/dicts.py) function with [JMESPath](https://jmespath.org/) key syntax. Chain operations on success, else continue with `None`
- A [`Mapper`](./pydian/mapper.py) class that performs post-processing cleanup on ["empty" values](./pydian/lib/util.py). For nuanced edge cases, condtionally [`DROP`](./pydian/lib/types.py) fields or [`KEEP`](./pydian/lib/util.py) specific values

(Experimental) If you're tired of writing one-off `lambda` functions, consider using:
- The `pydian.partials` module which provides (possibly) common 1-input, 1-output functions (`import pydian.partials as p`). A generic `p.do` wrapper creates a partial function which defaults parameters starting from the second parameter (`from functools import partial` starts from the first parameter.)

(Experimental) If you are working with `pl.DataFrame`s, you can use:
- A [`select`](./pydian/dataframes.py) function simple SQL-like syntax (`,`-delimited, `~` for conditionals, `*` to get all)
- Some functions for creating new dataframes (`left_join`, `inner_join`, `insert` for rows, `alter` for cols)

> Note: the DataFrame module is not included by default. To install, use:
> `pip install "pydian[dataframes]"`

## Examples

`dict`s: See [`get` tests](./tests/test_dicts.py) and [`Mapper` tests](./tests/test_mapper.py)

(Experimental) `pl.DataFrame`s: See [`select` tests](./tests/test_dataframes.py)

(Experimental) `pydian.partials`: See [`pydian.partial` tests](./tests/test_partials.py) or snippet below:

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

# Standardize how the partial functions are written for simpler management
assert p.equals(1)(1) == True
assert p.equivalent(False)(False) == True
assert get(source, 'some_values', apply=p.index(0), only_if=p.contains(350)) == 250
assert get(source, 'some_values', apply=p.index(0), only_if=p.contains(9000)) == None
assert get(source, 'some_values', apply=p.index(1)) == 350
assert get(source, 'some_values', apply=p.keep(2)) == [250, 350]
```

## Future Work

After 1.0, Pydian will be considered done (barring other community contributions 😃)

There may be further language support in the future (e.g. JS, Rust, Go, Julia, etc.) which could make this pattern even more useful (though still very much tbd!)

## Contact

Please submit a GitHub Issue for any bugs + feature requests 🙏
