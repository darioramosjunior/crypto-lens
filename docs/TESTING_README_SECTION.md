# Testing & CI/CD Setup

## Quick Start

### Run Tests Locally
```bash
# Install testing dependencies
pip install -r requirements.txt

# Run all tests
pytest tests/ -v

# Run with coverage report
pytest tests/ --cov --cov-report=html
```

### Setup Pre-commit Tests (Optional)
```bash
# Auto-run tests before each commit
python setup_pre_commit.py
```

## What's Tested

✅ **90+ unit and integration tests** covering:
- File and directory operations
- Configuration management
- Data loading and processing
- Logging functionality
- Coin data collection logic
- Market cap categorization
- CSV parsing and validation
- Multi-module integration workflows

## CI/CD Pipeline (GitHub Actions)

### Automatic Testing
Tests run automatically on:
- **Pull Requests** to `main`, `master`, or `develop`
- **Pushes** to `main`, `master`, or `develop`

### What Gets Checked
- ✅ All unit tests (Python 3.10 & 3.11)
- ✅ Code linting (flake8, pylint)
- ✅ Code formatting (black, isort)
- ✅ Code coverage reporting
- ✅ Integration with Codecov

### Status Badge
Passing: [![Tests](https://github.com/your-org/crypto-lens/actions/workflows/tests.yml/badge.svg)](https://github.com/your-org/crypto-lens/actions)

## Test Files

| File | Tests | Coverage |
|------|-------|----------|
| `tests/test_utils.py` | 30+ | FileUtility, ConfigManager, DataLoader, Math utilities |
| `tests/test_coin_data_collector.py` | 20+ | Symbol validation, filtering, market cap processing |
| `tests/test_logger_config.py` | 25+ | Logging, configuration, file paths |
| `tests/test_integration.py` | 15+ | Multi-module workflows and error handling |

## Common Commands

```bash
# Run all tests
pytest

# Run with verbose output
pytest -v

# Run specific test file
pytest tests/test_utils.py

# Run tests matching pattern
pytest tests/ -k "directory"

# Generate coverage report
pytest --cov --cov-report=html

# Run only failed tests
pytest --lf

# Run with detailed output
pytest -vv -s --tb=long
```

## Documentation

- 📖 [Quick Start Guide](TESTING_QUICKSTART.md) - Common commands and setup
- 📖 [Complete Testing Guide](docs/TESTING.md) - Detailed documentation
- 📖 [Setup Summary](TESTING_SETUP_SUMMARY.md) - What was created

## Requirements for Merge

All of these must pass before a PR can be merged:
- ✅ All tests pass
- ✅ Code passes linting checks
- ✅ GitHub Actions workflow succeeds

## Before Committing

```bash
# Run tests locally
pytest tests/ -v

# If setup with pre-commit:
git commit -m "your message"  # Tests run automatically
```

## Troubleshooting

**Tests fail locally but pass in CI?**
- Reinstall dependencies: `pip install -r requirements.txt`

**Pre-commit hook not running?**
- Reinstall: `python setup_pre_commit.py`

**Coverage report not generating?**
- Install pytest-cov: `pip install pytest-cov`

For more details, see [docs/TESTING.md](docs/TESTING.md)
