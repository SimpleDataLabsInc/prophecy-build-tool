## Testing and Formatting

### Running tests

The test layout is configured in the repo root `pytest.ini` (`testpaths = test`, `python_files = test_*.py`).

Run **everything** (legacy parity suite + new v2 tests):

```shell
python -m pytest
```

Run **only the new v2-focused suite** (under `test/v2/`, or anything marked `@pytest.mark.v2`):

```shell
python -m pytest test/v2/
# or
python -m pytest -m v2
```

Run **only legacy / pre-refactor tests** (marked `@pytest.mark.legacy`):

```shell
python -m pytest -m legacy
```

### Why legacy tests still exist

The files under `test/test_*.py` (excluding `test/v2/`) are kept as a **non-regression baseline** while the v2-first suite grows. They are tagged with `@pytest.mark.legacy` so CI can run them in a separate lane or serially if needed.

**Follow-up PR (not this refactor):** once the v2 suite has been stable on `main` for an agreed window, coverage has been mapped from legacy → v2, and reviewers are satisfied, legacy tests may be removed in a dedicated cleanup PR. Until then, **do not delete** legacy tests as part of feature work.

### Linting checks

* we use `black` to check linting and format our code.
* To check Black errors locally use

```shell
black --check src/ test/
```

* To format all code in `src/` and `test/` directories use

```shell
black -v src/ test/
```
