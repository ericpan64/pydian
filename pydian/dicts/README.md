# `dicts` Examples

`get` is the main driver -- get data in a functional way:
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

`Mapper` is a powerful tool for organizing data transformations. Use in-combination with `DROP` and `KEEP`:
```python
from pydian import get
from pydian.dicts import Mapper, DROP, KEEP

# Example source data
source = {
    "data": {
        "patient": {
            "id": 123,
            "name": "John Doe"
        }
    }
}

# Define a mapping function
def mapping(d: dict) -> dict:
    return {
        "patient_info": {
            "id": get(d, "data.patient.id"),
            "name": get(d, "data.patient.name"),
        },
        "extra_info": {
            "required_field": get(d, "something", drop_level=DROP.THIS_OBJECT)
        },  # Drop this field if the get fails
        "empty_static_value": "",
        "kept_static_value": KEEP("")
    }

# Create a Mapper instance
mapper = Mapper(mapping, remove_empty=True)

# Apply the mapping
result = mapper(source)

# The result:
assert result == {
    "patient_info": {
        "id": 123,
        "name": "John Doe"
    },
    "kept_static_value": ""
}
```
