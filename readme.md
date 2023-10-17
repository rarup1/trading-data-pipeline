# Trading data pipeline - Rupert Arup / Berenberg

## Description

This simple python application is a data pipeline that reads data from a parquet file, applies some transformations and writes the result to a parquet file.
It primarily uses pandas library as this offers in built support for some of the critical operations we need to perform such as joining market data with execution data using the asof method.


## How to run on docker

`docker build -t trading_data_pipeline .`

`docker run trading_data_pipeline`

## How to run on local

`pip install -r requirements.txt`

`python src/main.py`


# Testing

run `pytest` on the route folder

# Performance metrics

Performance on the data set:

```
2023-10-13 10:39:44,983 - root - INFO - Program started
2023-10-13 10:39:45,127 - root - INFO - Unique Venues in execution data: 6
2023-10-13 10:39:45,131 - root - INFO - Trading date(s): ['2022-09-02']
2023-10-13 10:39:45,306 - root - INFO - Number of executions: 4203
2023-10-13 10:39:45,306 - root - INFO - Number of executions with market data: 564
2023-10-13 10:39:45,976 - root - INFO - Data saved to processed.parquet
2023-10-13 10:39:45,976 - root - INFO - Time taken to run: 992.649 ms
```

Processing in under 1 second.


Increasing the data size by 10x, the time taken to run the program increases by 10x as well:

```
2023-10-13 11:45:22,336 - root - INFO - Program started
2023-10-13 11:45:22,422 - root - INFO - Unique Venues in execution data: 6
2023-10-13 11:45:22,444 - root - INFO - Trading date(s): ['2022-09-02']
2023-10-13 11:45:22,565 - root - INFO - Number of executions: 42030
2023-10-13 11:45:22,565 - root - INFO - Number of executions with market data: 5640
2023-10-13 11:45:32,934 - root - INFO - Data saved to processed.parquet
2023-10-13 11:45:32,934 - root - INFO - Time taken to run: 10597.67 ms
```

We would want to refactor the application to batch up executions above a certain size. We could also look to parallelize the application to take advantage of multiple cores on the machine. We would also want to look at the data set to see if there are any other optimizations we can make to the code.

The above was achieved by duplicating with the below code but has not been left in the src code anywhere:

```
# Define the duplication factor (e.g., 3 times)
duplication_factor = 10
# Duplicate the DataFrame
execution_df = pd.concat([execution_df] * duplication_factor, ignore_index=True)
```

# Next steps

- Add functionality to write to a database 
- Support incremental using database state and batching based on number of executions
- Add more tests