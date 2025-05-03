# pydian - <ins alt="p̲y̲">py</ins>thonic <ins alt="d̲">d</ins>ata <ins alt="i̲">i</ins>nterch<ins alt="a̲n̲">an</ins>ge

pydian is a pure Python library for readable and repeatable data mappings. pydian reduces boilerplate for data manipulation and provides a framework for expressive data wrangling.

Using pydian, developers can collaboratively and incrementally write data mappings that are expressive, safe, and reusable. Similar to how libraries like React were able to streamline UI components for frontend development, pydian aims to streamline data transformations for backend development.

## Installation
Install with pip:

```bash
pip install pydian
# For DataFrame support
pip install "pydian[dataframes]"
```

## Overview
pydian currently offers an ergonomic API for:
- Working with `dict` data
- Working with `polars.DataFrame` data
- Validating `dict` data using the `Rule` and `RuleGroup` framework

## Example
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

Additional constructs are added for more complex data operations (`Mapper`, `select` on Dataframes, `validate`, etc.).

See more examples in the [comprehensive tests](./tests) (definitely feel empowered to add more as well)!


## API Overview

The API is designed to be developer-friendly, prioritizing ergonomics and reliability (speed is decent, though be sure to benchmark your use-case).

### Implemented
- `pydian` -- working with `dict[str,Any]`
  - `get` -- grab data using JMESPath syntax
  - `Mapper` -- cleans-up empty values and allows complex logic with `DROP` and `KEEP`
- `pydian.partials` -- module with a bunch of common one-line functions. Good for codebase consistency
  - Suggested use: `import pydian.partials as p`
- `pydian.dataframes` -- working with `polars.DataFrame` (convert from pandas using the polars API)
  - NOTE: install this with `pip install "pydian[dataframes]"`
  - `select` -- grab dict data using simple SQL-like string syntax
    - `,`-delimited columns, `*` to get all, `:` for row filtering, `-> [ ... ]` for dict-unnesting, `-> { 'new_name': ... }` for renaming, `+>` for using `->` and also keep the original column
  - `join`, `union`, and `group_by` functions with SQL-like string syntax
- `pydian.validation` -- validating `dict[str, Any]` data via composition (interops with pydantic)
  - `Rule` and `RuleGroup` classes to group functions logically, and wraps results in `Ok` or `Err`
  - `validate` dict data using expressive, composible syntax

### Future work
The following are in-progress:
- `pydian.pipes` -- Module for running pipeline of operations
- `pydian.standards` -- Module for defining data standards and sharing mappings
- `pydian.io` -- Module for disk/database IO
- `pydian.ml` -- Module for ML training and inference
- Implement core library in lower-level language (e.g. Rust)
  - Port to JS


## Contact

Please submit a GitHub Issue for any bugs + feature requests 🙌 🙏
