import pandas as pd
import utils


def transform_execution_data(execution_df, market_df, ref_df) -> pd.DataFrame:
    """
    Transform the execution data.

    - Filter the execution data on the column Phase == CONTINUOUS_TRADING
    - Add column [‘side’], if quantity is negative, side = 2, if quantity is positive side = 1.
    - Complement the data with refdata.parquet ex_rf by adding column primary_ticket and primary_mic
    - Add listing_id to facilitate joining on market data
    - Set datatype for TradeTime
    - Filter executions on available market data listing_ids
    """

    # Filter on phase
    df = utils.filter_df_by_values(execution_df, "Phase", ["CONTINUOUS_TRADING"])

    # Add side column
    df["side"] = df["Quantity"].apply(lambda x: 2 if x < 0 else 1)

    # Add refdata columns and rename id to listing_id
    df = pd.merge(
        df,
        ref_df[["ISIN", "primary_mic", "primary_ticker", "id"]],
        on="ISIN",
        how="left",
    )
    df["listing_id"] = df["id"]
    df.drop(["id"], axis=1, inplace=True)

    df["TradeTime"] = pd.to_datetime(df["TradeTime"])

    # Filter on market data listing_ids
    listing_ids = market_df["listing_id"].unique()
    df = utils.filter_df_by_values(df, "listing_id", listing_ids)

    # Rename columns to underscore 
    df = utils.rename_columns_to_underscore(df)

    return df


def transform_market_data(market_df) -> pd.DataFrame:
    """
    Prepare the market data for the calculation.

    - Filter the market data on the column market_state == CONTINUOUS_TRADING
    - Set datatype for event_timestamp
    - keep relevant columns
    """

    df = utils.filter_df_by_values(market_df, "market_state", ["CONTINUOUS_TRADING"])
    df["event_timestamp"] = pd.to_datetime(df["event_timestamp"])
    df = df[["event_timestamp", "listing_id", "best_bid_price", "best_ask_price"]]

    return df
