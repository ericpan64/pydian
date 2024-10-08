# Contribution Guide

## Running Tests
First: `poetry install -E dataframes && poetry shell`

Then at the top level dir: `pytest`, or `pytest --cov` to view code coverage

> Note: to install the dataframes module, the `-E dataframes` flag is used for the extra `dataframes` section of `pyproject.toml`

## Code Formatting
This repo currently uses the following dev tools:
1. [`black`](https://github.com/psf/black) for code formatting (`black .`)
2. [`isort`](https://github.com/PyCQA/isort) for dependency sorting (`isort --profile black .`)
3. [`mypy`](https://github.com/python/mypy) for static type checking (`mypy --ignore-missing-imports .`)

You can run the individual commands and/or have them run via the [`pre-commit`](https://github.com/pre-commit/pre-commit) hooks (make sure you are using the pre-commit installed from the Poetry venv, i.e. from `poetry shell`, as opposed to another install).

## Opening a PR
Use this convention when creating a new branch: `{your_abbrv_name}/{contribution_description}`

E.g. `yname/general_update` or optionally `yname/1-fix_first_issue` if it's linked to an issue.

Thank you for contributing and working to keep things organized!

### Publishing to PyPI

```bash
poetry publish -u __token__ -p <token starting with pypi-...>
```

There's also a `poetry config` setting, though this seems to be fine for now!

## Complimentary Libraries
In addition to the standard library [itertools](https://docs.python.org/3/library/itertools.html), functional tools like [funcy](https://github.com/Suor/funcy) and [more-itertools](https://github.com/more-itertools/more-itertools) can improve development and make data transforms more consistent and elegant. [python-benedict](https://github.com/fabiocaccamo/python-benedict) is another handy library, and `benedict` objects can be used to facilitate mapping if an appropriate use-case comes up (though this was refactored-out since it provides more features than needed).
