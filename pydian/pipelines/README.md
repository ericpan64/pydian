# `pipelines` Examples

A `Pipe` specifies a series of operations:
```python
from result import Err, Ok
import pydian.partials as p
from pydian.pipelines.pipes import Pipe, Piper
from pydian.validation import IsType

# Define a Pipe that checks if the input is a string, multiplies it by 3, and checks again
triple_str = Pipe([IsType(str), p.multiply(3), IsType(str)])

# Test the Pipe with a valid string
result = triple_str("Hello!")
assert result == Ok("Hello!Hello!Hello!")

# Test the Pipe with an invalid integer
result = triple_str(10)
assert isinstance(result, Err)

# Define another Pipe that checks if the input is a string and strips whitespace
clean_str = Pipe([IsType(str), p.do(str.strip)])

# Combine Pipes into a Piper
str_pipeline = Piper(
    {
        triple_str: {Ok: Ok.unwrap, Err: p.echo},
        clean_str: {Ok: p.echo, Err: Err.unwrap_err},
    }
)

# Test the Piper with a valid string
result = str_pipeline(" Hello! ")
assert result == {
    triple_str: " Hello! Hello! Hello! ",
    clean_str: Ok("Hello!")
}

# Test the Piper with an invalid integer
result = str_pipeline(10)
assert result == {
    triple_str: Err(PipeState(0, IsType(str), {"args": (10,), "kwargs": {}}, None)),
    clean_str: PipeState(0, IsType(str), {"args": (10,), "kwargs": {}}, None),
}
```
