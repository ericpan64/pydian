# pydian.validation – dict data validation

This module provides tools to validate Python `dict[str, Any]` data via composable rules.

## API

- `validate(data: dict[str, Any], rules: RuleGroup | Rule) -> Ok | Err`  
  Validate input against one or more rules, returning `Ok(data)` or `Err(errors)`.

- `Rule(field: str, check: ConditionalCheck)`  
  Apply a single check to a field.

- `RuleGroup(*rules: Rule | RuleGroup)`  
  Group multiple rules together.

- `RC`, `RGC`  
  Type aliases for `Rule` and `RuleGroup` respectively.

### Specific Checks

- `IsRequired()` – field must exist.
- `IsOptional()` – field may be absent or None.
- `IsType(type_)` – value must be instance of `type_`.
- `InRange(min, max)` – numeric value between `min` and `max`.
- `InSet(*values)` – value must be one of `values`.
- `MinCount(n)` / `MaxCount(n)` – collection length constraints.

## Example

```python
from pydian.validation import validate, RuleGroup, Rule, IsRequired, InRange

rules = RuleGroup(
    Rule('age', IsRequired()),
    Rule('age', InRange(18, 99)),
)

result = validate({'age': 25}, rules)
if result.is_ok:
    print('Valid')
else:
    print('Errors:', result.unwrap_err())
```

## Contact

See the [root README](../README.md) for installation and project details.
