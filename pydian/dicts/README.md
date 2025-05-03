# pydian.dicts – dict data transformations

This module provides the core API for manipulating Python `dict` and nested data structures in a readable, repeatable way.

## API

- `get(source, key, default=None, apply=None, only_if=None, drop_level=None, flatten=False, strict=None)`
  Retrieves values from nested dicts/lists using dot/slice syntax, with safe defaults and optional transformations.

- `Mapper`
  A class for defining composable mappings with support for `DROP` and `KEEP` to clean up empty values.

## Example

```python
from pydian.dicts import get, Mapper, DROP

payload = {'foo': {'bar': 123}}
assert get(payload, 'foo.bar') == 123

with Mapper() as m:
    m.map('foo.bar', apply=str)
    transformed = m.run(payload)
```

## Contact

See the [root README](../README.md) for full project details and installation.
