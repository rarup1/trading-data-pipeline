import pandas as pd
import re


def filter_df_by_values(dataframe, column_name, values):
    """
    Filter a Pandas DataFrame by values in a specific column.

    Parameters:
    - dataframe (pd.DataFrame): The DataFrame to be filtered.
    - column_name (str): The name of the column to filter on.
    - values (list): A list of values to filter by.

    Returns:
    - pd.DataFrame: The filtered DataFrame.
    """
    if type(values) == str:
        raise TypeError(f"The values parameter must be a list/array not a {type(values)}")
    
    return dataframe[dataframe[column_name].isin(values)].copy()


def get_unique_column_values(df, column_name):
    """
    Get the unique values in a column of a Pandas DataFrame.

    Parameters:
    - df (pd.DataFrame): The DataFrame to get the unique values from.
    - column_name (str): The name of the column to get the unique values from.

    Returns:
    - int: The number of unique values in the column.
    """
    if column_name not in df.columns:
        raise ValueError(f"The column {column_name} does not exist in the DataFrame")

    return len(df[column_name].unique())


def merge_data_asof(execution_df, market_df, offset=0, suffix=""):
    """
    Merge the execution data and market data.
    """
    
    fields_to_rename = ['best_bid_price', 'best_ask_price']

    if fields_to_rename[0] not in market_df.columns:
        raise ValueError(f"The column {fields_to_rename[0]} does not exist in the market data")
    if fields_to_rename[1] not in market_df.columns:
        raise ValueError(f"The column {fields_to_rename[1]} does not exist in the market data")

    execution_df['tradetime_offset'] = execution_df['trade_time'] + pd.DateOffset(seconds=offset)
    df = pd.merge_asof(execution_df.sort_values('trade_time'), 
                       market_df.sort_values('event_timestamp'), 
                       by='listing_id',
                       left_on='tradetime_offset', 
                       right_on='event_timestamp')
    df[f'bbo_timestamp{suffix}'] = df['event_timestamp']
    
    df = df.rename(columns={field: field.replace("_price", suffix) for field in fields_to_rename})    
    df.drop(['tradetime_offset'], axis=1, inplace=True)
    df.drop(['event_timestamp'], axis=1, inplace=True)    
    return df


def rename_columns_to_underscore(df):
    """
    Rename columns in a Pandas DataFrame.
    """
    return df.rename(columns=lambda x: re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', x).lower())

def unique_dates(df, column_name):
    """
    Get the unique dates in a column of a Pandas DataFrame.
    """
    return pd.to_datetime(df[column_name]).dt.strftime('%Y-%m-%d').unique()


