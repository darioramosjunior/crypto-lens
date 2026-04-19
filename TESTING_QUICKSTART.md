# Quick Start: Testing & CI/CD for Crypto-Lens

## 5-Minute Setup

### Step 1: Install Testing Tools
```bash
pip install -r requirements.txt
```

### Step 2: Run Tests Locally
```bash
pytest tests/ -v
```

### Step 3: Setup Pre-commit Hooks (Optional)
```bash
python setup_pre_commit.py
```

**Done!** Tests now run automatically before commits and on GitHub for PRs.

---

## What's New?

### 📁 Test Files Created
```
tests/
├── __init__.py                  # Package init
├── conftest.py                  # Shared fixtures & config
├── test_utils.py               # 30+ tests for utility functions
├── test_coin_data_collector.py  # 20+ tests for coin logic
├── test_logger_config.py        # 25+ tests for logging/config
└── test_integration.py          # 15+ integration tests
```

**Total: 90+ unit and integration tests**

### ⚙️ Configuration Files
- `pytest.ini` - Pytest configuration
- `.github/workflows/tests.yml` - GitHub Actions CI/CD
- `setup_pre_commit.py` - Pre-commit hook installer

### 📖 Documentation
- `docs/TESTING.md` - Complete testing guide
- `scripts/pre-commit` - Pre-commit hook script

---

## How It Works

### Local Development (Pre-commit)
```
git add .
git commit -m "my changes"
    ↓
Pre-commit hook runs pytest
    ↓
Tests pass? → Commit succeeds
Tests fail? → Commit aborted (fix and try again)
```

### GitHub Pull Request (CI/CD)
```
Create PR or push to main/develop
    ↓
GitHub Actions triggers
    ↓
Runs tests on Python 3.10 & 3.11
Runs linting checks
Generates coverage report
    ↓
All pass? → PR can be merged
Any fail? → PR blocked (fix required)
```

---

## Common Commands

### Run All Tests
```bash
pytest
```

### Run Tests with Coverage
```bash
pytest --cov=. --cov-report=html
open htmlcov/index.html  # View coverage report
```

### Run Specific Test
```bash
pytest tests/test_utils.py::TestFileUtility::test_ensure_directory_exists_creates_directory -v
```

### Run Only New/Failed Tests
```bash
pytest --lf  # last failed
pytest --ff  # failed first
```

### Run with Detailed Output
```bash
pytest -vv -s --tb=long
```

### Fix Code Formatting
```bash
black .           # Format code
isort .           # Sort imports
```

---

## Test Structure

### Unit Tests (test_*.py files)
Test individual functions in isolation with mocked dependencies:

```python
def test_ensure_directory_exists_creates_directory(self):
    """Test that directory is created"""
    result = FileUtility.ensure_directory_exists(test_dir)
    assert result is True
    assert os.path.exists(test_dir)
```

### Integration Tests (test_integration.py)
Test how multiple modules work together:

```python
def test_load_coins_and_categories_workflow(self):
    """Test loading coins and categories from same file"""
    coins = get_coins(file)
    categories = get_categories(file)
    assert len(coins) == len(categories)
```

### Fixtures (conftest.py)
Reusable test setup:

```python
@pytest.fixture
def temp_dir():
    """Provides temporary directory"""
    temp = tempfile.mkdtemp()
    yield temp
    shutil.rmtree(temp)

def test_something(temp_dir):
    # temp_dir is automatically provided
    pass
```

---

## Tests by Module

| Module | File | Tests | Coverage |
|--------|------|-------|----------|
| utils.py | test_utils.py | 30+ | FileUtility, ConfigManager, DataLoader |
| coin_data_collector.py | test_coin_data_collector.py | 20+ | Symbol validation, filtering, batch processing |
| logger.py + config.py | test_logger_config.py | 25+ | Logging, config loading, path generation |
| Integration | test_integration.py | 15+ | Multi-module workflows |
| **Total** | **4 files** | **90+** | **All critical functions** |

---

## GitHub Actions Features

✅ **Multi-version testing** - Python 3.10 & 3.11
✅ **Coverage reports** - Codecov integration
✅ **Linting** - flake8, pylint, black
✅ **Import sorting** - isort validation
✅ **Caching** - Faster builds
✅ **Blocking** - Failed tests block merges

---

## Troubleshooting

| Problem | Solution |
|---------|----------|
| `pytest: command not found` | `pip install pytest` |
| Tests pass locally, fail in CI | `pip install -r requirements.txt` |
| Pre-commit hook fails | `python setup_pre_commit.py` |
| Want to skip pre-commit | `git commit --no-verify` (not recommended) |
| Coverage report not generated | `pip install pytest-cov` |

---

## Best Practices

### ✅ DO
- Write tests for new features
- Use descriptive test names: `test_function_scenario()`
- Include docstrings in test functions
- Test both success and failure cases
- Mock external API calls
- Commit tests with code changes

### ❌ DON'T
- Use `git commit --no-verify` to bypass tests
- Skip testing edge cases
- Mock too aggressively
- Hardcode file paths in tests
- Ignore failing tests
- Commit without running tests locally

---

## Next Steps

1. **First time?** Run `pytest` to see all tests pass
2. **Before committing?** Run `pytest` locally
3. **Creating PR?** GitHub Actions will test automatically
4. **Adding features?** Add tests to `tests/` directory
5. **Need coverage?** Run `pytest --cov --cov-report=html`

---

## Files Reference

```
crypto-lens/
├── tests/                          # Test directory
│   ├── conftest.py                 # Shared fixtures
│   ├── test_utils.py               # Utils tests
│   ├── test_coin_data_collector.py # Coin collector tests
│   ├── test_logger_config.py       # Logger/config tests
│   └── test_integration.py         # Integration tests
├── .github/
│   └── workflows/
│       └── tests.yml               # GitHub Actions CI/CD
├── scripts/
│   └── pre-commit                  # Pre-commit hook
├── docs/
│   └── TESTING.md                  # Detailed testing guide
├── pytest.ini                      # Pytest config
├── setup_pre_commit.py             # Hook setup script
└── requirements.txt                # Testing dependencies added
```

---

## More Info

- 📖 Read [docs/TESTING.md](docs/TESTING.md) for comprehensive guide
- 🔗 [pytest docs](https://docs.pytest.org/)
- 🔗 [GitHub Actions docs](https://docs.github.com/en/actions)
- 🔗 [Coverage.py docs](https://coverage.readthedocs.io/)

**Happy testing! 🎉**
