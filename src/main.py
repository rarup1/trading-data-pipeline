import pyarrow.parquet as pq
import datetime
import logging  # Import the logging module
import utils
import transform as tr
import calculate as calc
import config as cfg
import pandas as pd

# Configure logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

if __name__ == "__main__":

    logging.info("Program started")
    start_time = datetime.datetime.now()

    try:
        # read data from source (s3, sftp)
        execution_df = pq.read_table("data/executions.parquet").to_pandas()
        market_df = pq.read_table("data/marketdata.parquet").to_pandas()
        ref_df = pq.read_table("data/refdata.parquet").to_pandas()

        # Log venues and execution date
        logging.info(f"Unique Venues in execution data: {utils.get_unique_column_values(execution_df, 'Venue')}")
        logging.info(f"Trading date(s): {utils.unique_dates(execution_df, 'TradeTime')}")

        # Transform
        execution_df_t = tr.transform_execution_data(execution_df, market_df, ref_df)
        market_df_t = tr.transform_market_data(market_df)

        logging.info(f"Number of executions: {execution_df.shape[0]}")
        logging.info(f"Number of executions with market data: {execution_df_t.shape[0]}")

        # Calculate
        calc_df = calc.calculate_bbo(execution_df_t, market_df_t)
        calc_df = calc.calculate_mid_price(calc_df)
        calc_df = calc.calculate_slippage(calc_df)

        # Prepare the final output
        output_df = calc_df[cfg.FINAL_OUTPUT]

        # Save the data back to a parquet file ahead of loading to an OLAP DWH
        output_df.to_parquet("data/output/processed.parquet")
        # Save to make for easy viewing in excel
        output_df.to_csv("data/output/processed.csv", index=False)
        logging.info("Data saved to processed.parquet")

    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")

    end_time = datetime.datetime.now()
    logging.info(f"Time taken to run: {(end_time - start_time).total_seconds() * 1000} ms")