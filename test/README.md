## Testing and Formatting

### Running all tests
* we use `pytest` and `click.testing` to do unit testing
Please use following command to execute all tests

```shell
python -m pytest test/test_* -v
```

### Linting checks
* we use `black` to check linting and format our code.
* To check Black errors locally use
```shell
black --check src/ test/
```

* To format all code in `src/` and `test/` directories use, use
```shell
black -v src/ test/
```
