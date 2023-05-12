# HelloWorld Repository

### Running all tests
* we use `pytest` and `click.testing` to do unit testing
Please use following command to execute all tests

```shell
python -m pytest test/test_* -v
```

### Linting checks
* we use `flake8` to check linting and `black` to format our code.
* To check flake8 errors locally use
```shell
 flake8 . --count --exit-zero --statistics 
```

* To format all code in `src/` and `test/` directories use, use
```shell
black src test
```
