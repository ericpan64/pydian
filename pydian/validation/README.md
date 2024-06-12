# `validation` Examples

The main goal of this module is to have validation that is easy to specify and looks like the data being validated (here, dicts)!

`validate` is the main driver which requires a validation map (which at runtime converts to a `RuleGroup`):
```python
from typing import Any, Optional
from result import Err, Ok
from pydian.validation import RC, IsRequired, IsType, NotRequired, RuleGroup, validate

# Define validation rules
validation_rules = {
    "data": IsRequired() & {
        "patient": IsRequired() & {
            "id": IsRequired() & str,
            "active": IsRequired() & bool,
            "_some_new_key": str,  # implicitly optional
        }
    }
}

# Sample data to validate
sample_data = {
    "data": {
        "patient": {
            "id": "12345",
            "active": True,
            "_some_new_key": "optional_value"
        }
    }
}

# Check validation result
assert isinstance(validate(sample_data, validation_rules), Ok)

# Define stricter validation rules
strict_validation_rules = {
    "data": IsRequired() & {
        "patient": IsRequired() & {
            "id": IsRequired() & str,
            "active": IsRequired() & bool,
            "_some_new_key": IsRequired() & str,  # Now required
        }
    }
}

# Example of validation failure
invalid_data = {
    "data": {
        "patient": {
            "id": "12345",
            "active": True
            # "_some_new_key" is missing and required in this case
        }
    }
}

# Check validation result
assert isinstance(validate(invalid_data, strict_validation_rules), Err)
```
