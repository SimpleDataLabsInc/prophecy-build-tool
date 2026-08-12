## Testing and Formatting

### Test lanes

There are three logical lanes, enforced by pytest markers rather than folder paths:

| Lane | Marker selector | What it runs | Gates PR merge? |
| --- | --- | --- | --- |
| **Legacy parity** | `-m legacy` | Everything under `test/test_*.py` (pre-refactor baseline). | **Yes** (required check `pytest-legacy`) |
| **v2 fast** | `-m "v2 and fast"` | Unit + integration tests under `test/v2/` that don't run Maven/Spark/network. Safe under xdist. | **Yes** (required check `pytest-v2`) |
| **v2 e2e + slow** | `-m "e2e or maven or spark"` | Consolidated CLI-level e2e tests under `test/v2/e2e/` and bundle-driven git tests (`test/v2/test_versioning_git_e2e.py`, `test/v2/test_tagging_e2e.py`). Runs Maven/Spark/pip for real. | No — scheduled + push-to-main only |

Every test file under `test/v2/` must carry **both** `v2` **and exactly one of** `fast`/`e2e`. This is enforced at collection time by `test/conftest.py::pytest_collection_modifyitems`.

The `legacy` marker is **also** allowed on v2 tests, but only for tests targeting a deprecated subject (e.g. scala) — and only in combination with `e2e`. A `v2 + legacy + fast` test is a configuration error and will fail collection.

The canonical pattern for "same e2e against python and scala" is per-parameter `pytest.param(..., marks=pytest.mark.legacy)`, which keeps both languages in one test file and only marks the scala instance as legacy:

```python
LANGUAGES = [
    pytest.param("python", id="python"),
    pytest.param("scala",  id="scala", marks=pytest.mark.legacy),
]

@pytest.mark.parametrize("language", LANGUAGES)
def test_build_v2_produces_artefact_per_pipeline(..., language):
    ...
```

Selection then routes correctly without per-file boilerplate:

- `pytest -m "v2 and not legacy"` → python instance only
- `pytest -m "v2 and legacy"`     → scala instance only
- `pytest -m "v2 and fast"`       → neither (e2e is never in the fast lane)

### Running tests locally

Run **everything** (legacy + v2 fast + v2 e2e):

```shell
python -m pytest
```

Run **only the two required PR checks**, mirroring CI:

```shell
# What CI's `pytest-legacy` runs:
python -m pytest -m legacy

# What CI's `pytest-v2` runs:
python -m pytest -m "v2 and fast" -n auto
```

Run **only the slow / e2e lane** (requires Maven + Spark + network):

```shell
python -m pytest -m "e2e or maven or spark"
```

**Real-Databricks deploy e2e** (`test/v2/e2e/test_deploy_e2e.py`,
parametrized over python + scala) actually creates jobs in a real
Databricks workspace, then re-queries the Databricks API to confirm the
job exists, then deletes it. It is double-gated for safety:

1. Working creds: either `DATABRICKS_HOST` + `DATABRICKS_TOKEN` env
   vars, or a usable `~/.databrickscfg` profile (see
   `src/pbt/utils/databricks_creds.py`).
2. `PBT_E2E_DATABRICKS_OK=1` — explicit "yes, please create jobs in the
   workspace my creds point at". Without it, the test skips even when
   creds are present.

```shell
PBT_E2E_DATABRICKS_OK=1 \
  DATABRICKS_HOST=https://… \
  DATABRICKS_TOKEN=dapi… \
  python -m pytest test/v2/e2e/test_deploy_e2e.py -v
```

Each run suffixes job names with `-pbt-e2e-<lang>-<8-hex>` so concurrent runs
don't collide and a previous failed run can't masquerade as a current
success. Cleanup is best-effort in a `finally` block; if it fails, the
warning is printed so you can prune the leaked job manually.

### Why legacy tests still exist

The files under `test/test_*.py` (excluding `test/v2/`) are kept as a **non-regression baseline** while the v2-first suite grows. They are tagged with `@pytest.mark.legacy` so CI can run them in a separate required check for side-by-side comparison with the new v2 suite.

**Follow-up PR (not this refactor):** once the v2 suite has been stable on `main` for an agreed window, coverage has been mapped from legacy → v2, and reviewers are satisfied, legacy tests may be removed in a dedicated cleanup PR. Until then, **do not delete** legacy tests as part of feature work.

### File map

```
test/
├── conftest.py                     # shared fixtures (isolated env, cli_runner, per-worker Maven repo)
├── fakes.py                        # FakeRunner / FakeDeploy helpers
│
├── test_build.py                   # legacy — pbt build
├── test_deploy.py                  # legacy — pbt deploy
├── test_pipeline_sync.py           # legacy — rename-sync (unittest.TestCase)
├── test_tagging.py                 # legacy — pbt tag (clones HelloProphecy)
├── test_testing.py                 # legacy — pbt test
├── test_utils.py                   # legacy — databricks json rewrite
├── test_versioning.py              # legacy — pbt versioning (clones remote)
│
├── resources/
│   ├── HelloWorld/                 # realistic fixture project (python)
│   ├── HelloWorldBuildError/       # realistic fixture project that fails to build
│   ├── HelloProphecy/              # vendored copy of prophecy-samples/HelloProphecy
│   │   ├── prophecy/               #   — python project used by test/v2/e2e/ (python instance)
│   │   └── prophecy_scala/         #   — scala  project used by test/v2/e2e/ (scala instance)
│   ├── ProjectCreatedOn160523/     # scala fixture used by a few legacy tests
│   ├── versioning.bundle           # bundled git repo for versioning e2e (see below)
│   ├── tagging.bundle              # bundled git repo for tagging e2e
│   └── bundles/
│       ├── build_versioning_bundle.sh
│       └── build_tagging_bundle.sh
│
└── v2/
    ├── conftest.py                 # synthetic_project, fake_databricks_deploy,
    │                                 fake_test_python, git_bundle_repo fixtures
    ├── fixtures/
    │   └── artifactory/input.json  # input for test_artifactory_update.py
    │
    ├── test_artifactory_update.py  # ↔ ports test/test_utils.py
    ├── test_build_v2_units.py      # build_v2 filter plumbing (fake builder)
    ├── test_databricks_creds.py    # credentials resolver unit tests
    ├── test_deploy_v2_config.py    # deploy_v2 option plumbing
    ├── test_deploy_v2_units.py     # ↔ ports test/test_deploy.py (fake deploy)
    ├── test_python_probe.py        # python3/python discovery
    ├── test_rename_sync.py         # ↔ ports test/test_pipeline_sync.py
    ├── test_runner.py              # CommandRunner / FakeRunner self-tests
    ├── test_runner_integration.py  # CommandRunner ↔ PBTCli integration
    ├── test_semver_bump.py         # get_bumped_version unit tests
    ├── test_semver_sync.py         # version_check_sync unit tests
    ├── test_tagging_e2e.py         # ↔ ports test/test_tagging.py (bundle)
    ├── test_test_v2_coverage.py    # coverage/report artefact integration
    ├── test_test_v2_driver_path.py # --driver-library-path unit tests
    ├── test_test_v2_units.py       # ↔ ports pipeline-filter half of test_testing.py
    ├── test_validate_v2.py         # validate_v2 unit tests
    ├── test_versioning_cli.py      # ↔ ports non-git half of test_versioning.py
    ├── test_versioning_git_e2e.py  # ↔ ports git half of test_versioning.py (bundle)
    ├── test_wheel_build_runner.py  # wheel build runner unit tests
    │
    └── e2e/
        ├── conftest.py             # real_databricks_creds + suffix helper (shared)
        ├── test_build_e2e.py       # build_v2  parametrized over python+scala
        │                             — asserts each pipeline produces a .whl/.jar;
        │                               scala instance auto-marked legacy
        ├── test_deploy_e2e.py      # deploy_v2 parametrized over python+scala
        │                             — REAL Databricks API; opt-in via
        │                               PBT_E2E_DATABRICKS_OK=1, else skip
        └── test_test_e2e.py        # test_v2   parametrized over python+scala
```

### Realistic project fixtures (HelloProphecy)

The v2 e2e lane and the scala legacy lane both use the sibling pbt
projects in `test/resources/HelloProphecy/`:

- `prophecy/`       — python project (the `python` instance of every parametrized e2e in `test/v2/e2e/`)
- `prophecy_scala/` — scala  project (the `scala`  instance, auto-marked `legacy` per parametrize entry — scala is deprecated in PBT)

Each test gets its own isolated copy via the `helloprophecy_repo`
fixture in `test/conftest.py`, which returns
`(repo_root, python_project, scala_project)` over a `tmp_path`-scoped
`shutil.copytree`. There are **no clones at test time** — the repo is
vendored in-tree so the lane is offline, deterministic, and fast.

To refresh the vendored copy against the upstream
`prophecy-samples/HelloProphecy` repository's `main` branch:

```shell
tmp=$(mktemp -d) && \
  git clone --depth 1 --branch main \
    https://github.com/prophecy-samples/HelloProphecy.git "$tmp/HelloProphecy" && \
  rm -rf "$tmp/HelloProphecy/.git" && \
  rm -rf test/resources/HelloProphecy && \
  cp -R "$tmp/HelloProphecy" test/resources/HelloProphecy && \
  rm -rf "$tmp"
```

Then run `python -m pytest -m "v2 and e2e"` and `python -m pytest -m legacy`
to confirm nothing broke, and commit the diff.

### Regenerating the git bundles

Both bundles (`test/resources/versioning.bundle` and `test/resources/tagging.bundle`) are small, checked-in git repos that give the e2e lane reproducible offline fixtures. Regenerate from the shell scripts alongside them:

```shell
./test/resources/bundles/build_versioning_bundle.sh
./test/resources/bundles/build_tagging_bundle.sh
```

The bundles and their regeneration scripts are both tracked in git; editing a script without regenerating its bundle will be obvious in PR review.

Bundle layout (summary — see the scripts for the source of truth):

- **`versioning.bundle`** — one `main` branch at version `0.0.1` plus four sibling branches at `9999.0.0` / `0.0.0` / `not-a-version` / `0.0.1` so `--compare-to-target` and `--make-unique` tests have deterministic inputs.
- **`tagging.bundle`** — one `main` branch with both `prophecy/` (python) and `prophecy_scala/` sub-project layouts, so the tag CLI can be exercised against either project.

### Linting

We use `black` for formatting.

```shell
# check
black --check src/ test/
# format
black -v src/ test/
```
