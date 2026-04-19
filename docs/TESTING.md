# Testing Documentation for Crypto-Lens

## Overview

This document describes the testing setup for the Crypto-Lens project. The testing infrastructure includes:

- **Unit Tests**: Test individual functions in isolation
- **GitHub Actions CI/CD**: Automatic testing on pull requests and pushes
- **Pre-commit Hooks**: Local testing before commits
- **Code Quality Checks**: Linting and code formatting validation

## Test Structure

```
crypto-lens/
├── tests/
│   ├── __init__.py
│   ├── test_utils.py              # Tests for utility functions
│   ├── test_coin_data_collector.py # Tests for coin collection logic
│   ├── test_logger_config.py      # Tests for logging and config
│   └── conftest.py                # Pytest configuration (if needed)
├── pytest.ini                      # Pytest configuration
├── setup_pre_commit.py            # Setup script for pre-commit hooks
└── scripts/
    └── pre-commit                  # Pre-commit hook script
```

## Installation & Setup

### 1. Install Testing Dependencies

```bash
pip install -r requirements.txt
```

This installs testing packages:
- `pytest` - Test framework
- `pytest-cov` - Code coverage reporting
- `pytest-asyncio` - Async test support
- `pytest-mock` - Mocking utilities
- `pylint`, `flake8` - Code linting
- `black`, `isort` - Code formatting

### 2. Setup Pre-commit Hooks (Optional but Recommended)

Pre-commit hooks automatically run tests before each commit.

```bash
python setup_pre_commit.py
```

This will install the pre-commit hook in your `.git/hooks/` directory.

## Running Tests

### Run All Tests

```bash
pytest
```

or with verbose output:

```bash
pytest tests/ -v
```

### Run Specific Test File

```bash
pytest tests/test_utils.py -v
```

### Run Specific Test Class

```bash
pytest tests/test_utils.py::TestFileUtility -v
```

### Run Specific Test Function

```bash
pytest tests/test_utils.py::TestFileUtility::test_ensure_directory_exists_creates_directory -v
```

### Run Tests with Coverage Report

```bash
pytest tests/ --cov=. --cov-report=html --cov-report=term-missing
```

This generates an HTML coverage report in `htmlcov/index.html`.

## Test Organization

### test_utils.py
Tests for utility classes in `utils.py`:
- **TestFileUtility**: Tests for file and directory operations
- **TestConfigManager**: Tests for configuration management
- **TestDataLoaderUtility**: Tests for data loading from CSV
- **TestMathUtility**: Tests for mathematical operations

### test_coin_data_collector.py
Tests for coin collection logic in `coin_data_collector.py`:
- **TestSymbolValidation**: Tests for coin symbol validation
- **TestCoinFiltering**: Tests for filtering invalid coins
- **TestMarketCapDataProcessing**: Tests for market cap data processing
- **TestBatchProcessing**: Tests for batch processing logic

### test_logger_config.py
Tests for logging and configuration:
- **TestLoggerFunctions**: Tests for log event creation
- **TestConfigFunctions**: Tests for config management
- **TestConfigFileLoading**: Tests for config file parsing

## GitHub Actions Workflow

The project includes automated testing through GitHub Actions. Tests run automatically on:
- **Pull Requests** to main/master/develop branches
- **Pushes** to main/master/develop branches

### Workflow File
Location: `.github/workflows/tests.yml`

**Jobs:**
1. **test**: Runs pytest on Python 3.10 and 3.11
2. **lint**: Runs code quality checks (flake8, black, isort, pylint)

### Workflow Features
- ✅ Multi-version Python testing
- ✅ Dependency caching for faster builds
- ✅ Coverage report generation
- ✅ Codecov integration
- ✅ Code linting and formatting checks

## Pre-commit Hook

The pre-commit hook runs pytest before each commit. If tests fail, the commit is aborted.

### Manual Trigger
You can also manually run the pre-commit checks:

```bash
./scripts/pre-commit
```

### Bypass Pre-commit Hook (Not Recommended)
```bash
git commit --no-verify
```

## Test Coverage

Current test coverage includes:

### Utilities (utils.py)
- File operations (create, check existence)
- Configuration management
- Data loading from CSV
- Math utilities (normalization, percentage change)

### Coin Data Collector (coin_data_collector.py)
- Symbol validation
- Coin filtering logic
- Market cap data processing
- Batch processing for API calls

### Logger & Config (logger.py, config.py)
- Log event creation and formatting
- Directory creation
- File path generation
- Config loading and validation

## Best Practices

### 1. Write Tests for New Features
When adding new functionality:
1. Create a test function in the appropriate test file
2. Use descriptive names: `test_<function>_<scenario>`
3. Include docstrings explaining what the test verifies

Example:
```python
def test_my_function_handles_edge_case(self):
    """Test that my_function handles edge case correctly"""
    result = my_function(edge_case_input)
    assert result == expected_output
```

### 2. Use Fixtures for Common Setup
Create reusable test setup with pytest fixtures:

```python
@pytest.fixture
def temp_dir():
    """Provide a temporary directory for tests"""
    temp = tempfile.mkdtemp()
    yield temp
    shutil.rmtree(temp)

def test_with_fixture(temp_dir):
    # temp_dir is automatically provided
    pass
```

### 3. Mock External Dependencies
Use `unittest.mock` to mock external API calls:

```python
@patch('external_module.api_call')
def test_with_mock(mock_api):
    mock_api.return_value = {'result': 'success'}
    result = function_using_api()
    assert result is not None
```

### 4. Test Both Success and Failure Cases
For each function, test:
- ✅ Happy path (normal use)
- ✅ Boundary conditions
- ✅ Error handling (invalid inputs, missing files, etc.)

## Troubleshooting

### Issue: "pytest: command not found"
**Solution**: Install pytest
```bash
pip install pytest
```

### Issue: Tests fail but work locally
**Solution**: Ensure all dependencies are installed
```bash
pip install -r requirements.txt
```

### Issue: Pre-commit hook not working
**Solution**: Run setup script again
```bash
python setup_pre_commit.py
```

### Issue: Coverage report not generated
**Solution**: Install pytest-cov
```bash
pip install pytest-cov
```

## CI/CD Integration

### Required for PR Merge
All these must pass before merging:
- ✅ All unit tests pass
- ✅ Code passes linting checks
- ✅ No new critical issues introduced

### View Results
1. Go to pull request on GitHub
2. Check the "Checks" section
3. Click on failed checks to see detailed logs

## Continuous Improvement

### Adding Coverage Goals
To enforce minimum coverage requirements, add to GitHub Actions:

```yaml
- name: Check coverage
  run: pytest tests/ --cov --cov-fail-under=80
```

### Adding More Comprehensive Tests
Future test improvements:
- Integration tests with mock data
- Performance/load testing
- End-to-end testing with test fixtures
- API response validation tests

## References

- [pytest Documentation](https://docs.pytest.org/)
- [GitHub Actions Documentation](https://docs.github.com/en/actions)
- [Python unittest.mock](https://docs.python.org/3/library/unittest.mock.html)
- [Coverage.py](https://coverage.readthedocs.io/)

## Questions?

For issues or questions about testing:
1. Check this documentation
2. Run tests with `-v` (verbose) flag for more details
3. Check GitHub Actions logs for CI failures
