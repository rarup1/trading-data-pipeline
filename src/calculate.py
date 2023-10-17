import pandas as pd
import config as cfg
import utils


def calculate_bbo(execution_df, execution_md) -> pd.DataFrame:
    """
    Calculate the best bid and best ask at execution time and 1s before and after.

    - Merge the execution data and market data on listing_id and TradeTime
    - Merge the execution data and market data on listing_id and TradeTime - 1s
    - Merge the execution data and market data on listing_id and TradeTime + 1s
    - Merge the three datasets 
    """

    config = [
        {"offset": 0, "suffix": ""},
        {"offset": -1, "suffix": "_min_1s"},
        {"offset": 1, "suffix": "_1s"},
    ]

    df_list = []
    for c in config:
        df_list.append(
            utils.merge_data_asof(
                execution_df, execution_md, offset=c["offset"], suffix=c["suffix"]
            )
        )

    merged_df = df_list[0]
    for df in df_list[1:]:
        merged_df = pd.merge(merged_df, df, on=cfg.EXECUTION_COLUMNS, how="left")

    return merged_df


def slippage_formula(row):
    """
    Formula to calculate the slippage.
    """
    if row['side'] == 1:
        return (row['price'] - row['best_bid']) / (row['best_ask'] - row['best_bid'])
    else:
        return (row['best_ask'] - row['price']) / (row['best_ask'] - row['best_bid'])


def calculate_slippage(df) -> pd.DataFrame:
    """
    Calculate the slippage on a df
    """
    columns_to_check = ['price', 'best_bid', 'best_ask']
    if not (all(col in df.columns for col in columns_to_check)):
        raise ValueError(f"The columns {columns_to_check} do not exist in the DataFrame")

    df["slippage"] = df.apply(slippage_formula, axis=1)
    return df


def calculate_mid_price(df) -> pd.DataFrame:
    """
    Calculate the mid prices
    """
    df["mid_price"] = (df["best_bid"] + df["best_ask"]) / 2
    df["mid_price_min_1s"] = (df["best_bid_min_1s"] + df["best_ask_min_1s"]) / 2
    df["mid_price_1s"] = (df["best_bid_1s"] + df["best_ask_1s"]) / 2
    return df    

    
