# Data Lake

## Introduction
A startup called Sparkify developed a music streaming app, which saves user activity in JSON-files and songs metadata in the cloud. Their analytics team wants to analyze these data.

The aim of the project is to create a data lake on AWS pulling data from JSON-files located on Amazon S3 into analytical tables arranged in a star schema using Spark and write these tables back to Amazon S3 as parquet files.

## Project Structure
This project icludes 3 following files:<br>
1) **dl.cfg** - configuration file<br>
2) **etl.py** - reads files from Amazon S3, processes these files into analitical tables with Spark, and writes these tables back to Amazon S3 as parquet files<br>
3) **README.md** - this file<br>

## Analytics Schema
The analytics schema is a star schema optimized for queries on song play analysis. It consists of the fact table<br> `songplays`<br> and the dimension tables:<br> `artists`<br> `songs`<br> `time`<br> `users`<br> The structure is represented by the following diagram:
![sparkifydb](sparkifydb.png)

## Run Project
To run the project, modify the configuration file `dl.cfg` and then execute the following command in Terminal:<br>
`python3 etl.py`