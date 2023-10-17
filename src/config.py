EXECUTION_COLUMNS = [
        "trade_id",
        "isin",
        "listing_id",
        "trade_time",
        "currency",
        "venue",
        "price",
        "phase",
        "quantity",
        "side",
        "primary_mic",
        "primary_ticker"
    ]

MARKET_COLUMNS = [
        "event_timestamp",
        "listing_id",
        "best_bid_price",
        "best_ask_price",
        "market_state"
    ]

REF_COLUMNS = [
        "id",
        "isin",
        "primary_mic",
        "primary_ticker"
    ]

 
FINAL_OUTPUT = [
        "trade_id",
        "isin",
        "listing_id",
        "primary_ticker",
        "currency",
        "venue",
        "price",
        "phase",
        "quantity",
        "side",
        "primary_mic",
        "trade_time",
        "bbo_timestamp",
        "best_bid",
        "best_ask",
        "best_bid_min_1s",
        "best_ask_min_1s",
        "bbo_timestamp_min_1s",
        "best_bid_1s",
        "best_ask_1s",
        "bbo_timestamp_1s",
        "mid_price",
        "mid_price_min_1s",
        "mid_price_1s",
        "slippage"
    ]
   
