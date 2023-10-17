import pandas as pd
import re
import pytest
import os 
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), os.path.pardir)))
import utils

@pytest.fixture
def sample_dataframe():
    data = {
        'column1': [1, 2, 3, 4, 5],
        'column2': ['A', 'B', 'C', 'D', 'E']
    }
    return pd.DataFrame(data)

def test_filter_df_by_values(sample_dataframe):
    filtered_df = utils.filter_df_by_values(sample_dataframe, 'column1', [2, 4])
    assert len(filtered_df) == 2

def test_get_unique_column_values(sample_dataframe):
    unique_values = utils.get_unique_column_values(sample_dataframe, 'column2')
    assert unique_values == 5  # All values are unique

    
@pytest.mark.parametrize("offset, expected_output", [(-5, 100), (0, 200), (5, 300)])
def test_merge_asof_util(offset, expected_output):
    
    # mock data
    execution_df = pd.DataFrame({
    'listing_id': [1],
    'trade_time': pd.to_datetime(['2023-10-10 12:00:00'])
    })

    market_df = pd.DataFrame({
    'listing_id': [1, 1, 1],
    'event_timestamp': pd.to_datetime(['2023-10-10 11:59:54', '2023-10-10 11:59:59', '2023-10-10 12:00:04']),
    'best_bid_price': [100, 200, 300],
    'best_ask_price': [110, 210, 310]
    })

    result = utils.merge_data_asof(execution_df, market_df, offset=offset)
    assert result['best_bid'][0] == expected_output

def test_rename_columns_to_underscore():
    df = pd.DataFrame({'CamelCaseColumn': [1, 2, 3]})
    renamed_df = utils.rename_columns_to_underscore(df)
    assert 'camel_case_column' in renamed_df.columns

def test_unique_dates():
    df = pd.DataFrame({'date_column': ['2023-10-10 12:00:00', '2023-10-10 12:15:00', '2023-10-11 12:30:00']})
    unique_date_values = utils.unique_dates(df, 'date_column')
    assert '2023-10-10' in unique_date_values
    assert '2023-10-11' in unique_date_values
