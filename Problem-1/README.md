# Problem 1 – AgriSense

## Problem Statement

Implement a Hadoop MapReduce program to join agricultural yield data with weather data based on **Region** and **Season**.

## Input

- `yield.csv` – Contains agricultural yield data.
- `weather.csv` – Contains weather data.

The common key used for joining the datasets is **Region + Season**.

## Output

The Reducer produces the joined yield and weather information for matching Region-Season combinations.

## Files

- `RegionSeasonDriver.java` – Runs the MapReduce job.
- `RegionSeasonMapper.java` – Processes input records and creates Region-Season keys.
- `RegionSeasonReducer.java` – Joins matching records and generates the output.
- `yield.csv` – Yield input data.
- `weather.csv` – Weather input data.

## Technologies Used

- Java
- Hadoop MapReduce
- HDFS
- YARN

## How to Run

Run the MapReduce job using:

```bash
hadoop jar /tmp/agrisense-problem1.jar RegionSeasonDriver /agrisense/input/yield.csv /agrisense/output/problem1
```

To view the output:

```bash
hdfs dfs -cat /agrisense/output/problem1/part-r-00000
```