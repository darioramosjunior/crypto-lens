"""
Example: How to Write Tests for New Features

This file shows best practices for writing tests when adding new functionality
to the Crypto-Lens project.
"""

import pytest
import os
import tempfile
import shutil
from unittest.mock import patch, MagicMock


# ============================================================================
# EXAMPLE 1: Testing a Simple Utility Function
# ============================================================================

class MyNewUtility:
    """Example utility class for demonstration"""
    
    @staticmethod
    def calculate_average(numbers: list) -> float:
        """Calculate average of numbers"""
        if not numbers:
            return 0.0
        return sum(numbers) / len(numbers)
    
    @staticmethod
    def validate_positive(value: float) -> bool:
        """Check if value is positive"""
        return value > 0


class TestMyNewUtility:
    """Tests for MyNewUtility - SIMPLE EXAMPLE"""
    
    def test_calculate_average_with_numbers(self):
        """Test calculating average with valid numbers"""
        result = MyNewUtility.calculate_average([1, 2, 3, 4, 5])
        assert result == 3.0
    
    def test_calculate_average_with_empty_list(self):
        """Test calculating average with empty list"""
        result = MyNewUtility.calculate_average([])
        assert result == 0.0
    
    def test_calculate_average_with_single_number(self):
        """Test calculating average with single number"""
        result = MyNewUtility.calculate_average([5])
        assert result == 5.0
    
    def test_calculate_average_with_negative_numbers(self):
        """Test calculating average with negative numbers"""
        result = MyNewUtility.calculate_average([-5, -10, 5])
        assert result == -10/3


# ============================================================================
# EXAMPLE 2: Testing File Operations
# ============================================================================

class MyFileHandler:
    """Example file handler"""
    
    @staticmethod
    def save_data(filename: str, data: str) -> bool:
        """Save data to file"""
        try:
            with open(filename, 'w') as f:
                f.write(data)
            return True
        except Exception:
            return False
    
    @staticmethod
    def load_data(filename: str) -> str:
        """Load data from file"""
        try:
            with open(filename, 'r') as f:
                return f.read()
        except FileNotFoundError:
            return None


class TestMyFileHandler:
    """Tests for MyFileHandler - FILE OPERATIONS"""
    
    def setup_method(self):
        """Setup temporary directory for each test"""
        self.temp_dir = tempfile.mkdtemp()
    
    def teardown_method(self):
        """Cleanup temporary directory after each test"""
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)
    
    def test_save_and_load_data(self):
        """Test saving and loading data"""
        filepath = os.path.join(self.temp_dir, "test.txt")
        test_data = "Hello World"
        
        # Save
        assert MyFileHandler.save_data(filepath, test_data) is True
        
        # Load
        loaded_data = MyFileHandler.load_data(filepath)
        assert loaded_data == test_data
    
    def test_load_nonexistent_file_returns_none(self):
        """Test loading non-existent file returns None"""
        filepath = os.path.join(self.temp_dir, "nonexistent.txt")
        result = MyFileHandler.load_data(filepath)
        assert result is None


# ============================================================================
# EXAMPLE 3: Testing with Mocks (External Dependencies)
# ============================================================================

class APIClient:
    """Example API client"""
    
    def fetch_data(self, endpoint: str) -> dict:
        """Fetch data from API"""
        import requests
        response = requests.get(f"https://api.example.com/{endpoint}")
        return response.json()


class MyDataProcessor:
    """Example data processor using external API"""
    
    def __init__(self, api_client: APIClient):
        self.api = api_client
    
    def process_user_data(self, user_id: str) -> dict:
        """Fetch and process user data"""
        data = self.api.fetch_data(f"users/{user_id}")
        if data:
            data['processed'] = True
            return data
        return None


class TestMyDataProcessor:
    """Tests for MyDataProcessor - MOCKING EXTERNAL DEPENDENCIES"""
    
    def test_process_user_data_with_mock_api(self):
        """Test processing data with mocked API"""
        # Create mock API
        mock_api = MagicMock()
        mock_api.fetch_data.return_value = {'name': 'John', 'age': 30}
        
        # Create processor with mock
        processor = MyDataProcessor(mock_api)
        
        # Process data
        result = processor.process_user_data("123")
        
        # Verify
        assert result['name'] == 'John'
        assert result['processed'] is True
        mock_api.fetch_data.assert_called_once_with("users/123")
    
    def test_process_user_data_with_empty_response(self):
        """Test processing when API returns empty"""
        # Mock API to return empty dict
        mock_api = MagicMock()
        mock_api.fetch_data.return_value = {}
        
        processor = MyDataProcessor(mock_api)
        result = processor.process_user_data("123")
        
        # Should return None for empty response
        assert result is None


# ============================================================================
# EXAMPLE 4: Using Pytest Fixtures
# ============================================================================

@pytest.fixture
def sample_data():
    """Fixture providing sample data"""
    return {
        'coins': ['BTC', 'ETH', 'BNB'],
        'prices': [45000, 2500, 320]
    }


@pytest.fixture
def temp_csv_file(temp_dir):
    """Fixture providing temporary CSV file"""
    import pandas as pd
    
    csv_path = os.path.join(temp_dir, "data.csv")
    df = pd.DataFrame({
        'symbol': ['BTC', 'ETH', 'BNB'],
        'price': [45000, 2500, 320]
    })
    df.to_csv(csv_path, index=False)
    return csv_path


class TestWithFixtures:
    """Tests using fixtures - FIXTURE USAGE"""
    
    def test_with_sample_data(self, sample_data):
        """Test using fixture data"""
        assert len(sample_data['coins']) == 3
        assert sample_data['prices'][0] == 45000
    
    def test_with_temp_csv(self, temp_csv_file):
        """Test with temporary CSV file"""
        import pandas as pd
        df = pd.read_csv(temp_csv_file)
        
        assert len(df) == 3
        assert df['symbol'].tolist() == ['BTC', 'ETH', 'BNB']


# ============================================================================
# EXAMPLE 5: Testing Error Cases and Edge Conditions
# ============================================================================

class MyDataValidator:
    """Example data validator"""
    
    @staticmethod
    def validate_email(email: str) -> bool:
        """Validate email format"""
        if not email or '@' not in email:
            return False
        return True
    
    @staticmethod
    def validate_price(price: float) -> bool:
        """Validate price is positive"""
        return isinstance(price, (int, float)) and price > 0


class TestMyDataValidator:
    """Tests for MyDataValidator - ERROR CASES"""
    
    def test_validate_email_valid(self):
        """Test valid email"""
        assert MyDataValidator.validate_email("user@example.com") is True
    
    def test_validate_email_invalid_no_at(self):
        """Test email without @ symbol"""
        assert MyDataValidator.validate_email("userexample.com") is False
    
    def test_validate_email_empty_string(self):
        """Test empty email"""
        assert MyDataValidator.validate_email("") is False
    
    def test_validate_email_none(self):
        """Test None email"""
        assert MyDataValidator.validate_email(None) is False
    
    def test_validate_price_positive(self):
        """Test positive price"""
        assert MyDataValidator.validate_price(100.50) is True
    
    def test_validate_price_zero(self):
        """Test zero price"""
        assert MyDataValidator.validate_price(0) is False
    
    def test_validate_price_negative(self):
        """Test negative price"""
        assert MyDataValidator.validate_price(-50) is False
    
    def test_validate_price_invalid_type(self):
        """Test invalid type"""
        assert MyDataValidator.validate_price("100") is False


# ============================================================================
# EXAMPLE 6: Testing Multiple Scenarios
# ============================================================================

class TestDataScenariosParametrized:
    """Tests using parametrization - MULTIPLE SCENARIOS"""
    
    @pytest.mark.parametrize("input_value,expected", [
        (1, True),
        (0, False),
        (-1, False),
        (100, True),
    ])
    def test_validate_price_multiple_cases(self, input_value, expected):
        """Test validation with multiple input cases"""
        result = MyDataValidator.validate_price(input_value)
        assert result == expected
    
    @pytest.mark.parametrize("email,expected", [
        ("user@example.com", True),
        ("invalid.email", False),
        ("", False),
        ("test@test.co.uk", True),
    ])
    def test_validate_email_multiple_cases(self, email, expected):
        """Test email validation with multiple cases"""
        result = MyDataValidator.validate_email(email)
        assert result == expected


# ============================================================================
# GUIDELINES FOR WRITING TESTS
# ============================================================================

"""
BEST PRACTICES FOR WRITING TESTS:

1. TEST NAMING
   ✓ test_<function_name>_<scenario>
   ✓ test_calculate_average_with_numbers
   ✓ test_load_file_when_missing
   
2. ORGANIZATION
   ✓ Group related tests in classes
   ✓ Use setup_method() and teardown_method()
   ✓ Use fixtures for reusable setup
   
3. ASSERTIONS
   ✓ One assertion per test (ideally)
   ✓ Use assert with clear conditions
   ✓ Use assert with meaningful message: assert x == y, "message"
   
4. COVERAGE
   ✓ Test happy path (normal use)
   ✓ Test edge cases (empty, zero, None, etc.)
   ✓ Test error conditions (invalid input, missing file)
   
5. MOCKING
   ✓ Mock external API calls
   ✓ Mock file system access when needed
   ✓ Use unittest.mock.patch or MagicMock
   
6. DOCUMENTATION
   ✓ Use docstrings for classes and functions
   ✓ Explain what each test verifies
   ✓ Include docstrings in test methods
   
7. COMMON PATTERN
   - Setup test data (arrange)
   - Call the function (act)
   - Verify results (assert)
   
   def test_something(self):
       # ARRANGE
       test_input = [1, 2, 3]
       expected = 2.0
       
       # ACT
       result = MyClass.calculate_average(test_input)
       
       # ASSERT
       assert result == expected
"""

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
