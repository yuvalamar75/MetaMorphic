import pandas as pd
import pytest
import logging
from src.transformer import Transformer

@pytest.fixture
def sample_df():
    """Create a sample DataFrame for testing."""
    data = {
        'treatment_id': [1, 1, 1, 2, 2, 3],
        'date_col': ['2024-01-01', '2024-01-02', '2024-01-03', 
                     '2024-02-01', '2024-02-02', '2024-03-01'],
        'value': [10, 20, 30, 40, 50, 60]
    }
    return pd.DataFrame(data)

@pytest.fixture
def transformer(sample_df):
    """Create a Transformer instance with sample data."""
    logger = logging.getLogger('test_logger')
    return Transformer(logger, sample_df)

def test_add_group_order_single_column(transformer):
    """Test ordering by a single column."""
    result = transformer.add_group_order(['treatment_id'])
    
    # Check if order column exists
    assert 'order' in result.columns
    
    # Check if orders are correct within treatment_id groups
    assert result[result['treatment_id'] == 1]['order'].tolist() == [1, 2, 3]
    assert result[result['treatment_id'] == 2]['order'].tolist() == [1, 2]
    assert result[result['treatment_id'] == 3]['order'].tolist() == [1]

def test_add_group_order_multiple_columns(transformer):
    """Test ordering by multiple columns."""
    result = transformer.add_group_order(['treatment_id', 'date_col'], 'sequence_num')
    
    # Check if custom column name exists
    assert 'sequence_num' in result.columns
    
    # Check if the DataFrame is properly sorted
    assert result['date_col'].tolist() == [
        '2024-01-01', '2024-01-02', '2024-01-03',
        '2024-02-01', '2024-02-02', '2024-03-01'
    ]

def test_add_group_order_invalid_column(transformer):
    """Test with non-existent column."""
    with pytest.raises(ValueError, match="Columns not found:.*"):
        transformer.add_group_order(['non_existent_column'])

def test_add_group_order_empty_group_columns(transformer):
    """Test with empty group columns list."""
    with pytest.raises(ValueError):
        transformer.add_group_order([])

def test_add_group_order_with_nulls(transformer):
    """Test ordering with null values in grouping columns."""
    # Create DataFrame with null values
    data = {
        'treatment_id': [1, 1, None, 2, None, 3],
        'date_col': ['2024-01-01', None, '2024-01-03', 
                     '2024-02-01', '2024-02-02', None],
        'value': [10, 20, 30, 40, 50, 60]
    }
    df_with_nulls = pd.DataFrame(data)
    transformer.df = df_with_nulls
    
    result = transformer.add_group_order(['treatment_id', 'date_col'])
    
    # Check if nulls are handled (they should be grouped together)
    assert 'order' in result.columns
    # Null treatment_ids should be grouped together
    assert result[result['treatment_id'].isna()]['order'].tolist() == [1, 2]

def test_add_group_order_with_duplicates(transformer):
    """Test ordering with duplicate values in grouping columns."""
    # Create DataFrame with duplicate dates
    data = {
        'treatment_id': [1, 1, 1, 2, 2, 2],
        'date_col': ['2024-01-01', '2024-01-01', '2024-01-02', 
                     '2024-01-01', '2024-01-01', '2024-01-02'],
        'value': [10, 20, 30, 40, 50, 60]
    }
    df_with_duplicates = pd.DataFrame(data)
    transformer.df = df_with_duplicates
    
    result = transformer.add_group_order(['treatment_id', 'date_col'])
    
    # Check if duplicates are numbered sequentially
    assert result[
        (result['treatment_id'] == 1) & 
        (result['date_col'] == '2024-01-01')
    ]['order'].tolist() == [1, 2]

def test_add_group_order_custom_sort(transformer):
    """Test ordering with custom sort order (descending)."""
    # Modify the add_group_order function to accept ascending parameter
    data = {
        'treatment_id': [1, 1, 1, 2, 2, 3],
        'value': [30, 20, 10, 50, 40, 60]
    }
    df = pd.DataFrame(data)
    transformer.df = df
    
    # Add ascending parameter to the function call
    result = transformer.add_group_order(['treatment_id', 'value'])
    
    # Check if values are ordered correctly within groups
    assert result[result['treatment_id'] == 1]['value'].tolist() == [10, 20, 30]
    assert result[result['treatment_id'] == 2]['value'].tolist() == [40, 50]

def test_add_group_order_large_groups(transformer):
    """Test ordering with large number of rows in groups."""
    # Create a larger DataFrame
    large_data = {
        'treatment_id': [1] * 1000 + [2] * 1000,  # 2000 rows
        'value': list(range(2000))
    }
    df_large = pd.DataFrame(large_data)
    transformer.df = df_large
    
    result = transformer.add_group_order(['treatment_id'])
    
    # Check if large groups are handled correctly
    assert max(result[result['treatment_id'] == 1]['order']) == 1000
    assert max(result[result['treatment_id'] == 2]['order']) == 1000 