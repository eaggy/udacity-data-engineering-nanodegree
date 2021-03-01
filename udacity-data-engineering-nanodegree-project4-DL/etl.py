import configparser
from datetime import datetime
import os
import boto3
from botocore.exceptions import ParamValidationError
from botocore.exceptions import ClientError
from pyspark.sql import Window, SparkSession
from pyspark.sql.functions import udf, col, year, month, dayofmonth, dayofweek, hour, weekofyear, date_format, to_timestamp, max, monotonically_increasing_id
from pyspark.sql.types import StructType as R, StructField as Fld, DoubleType as Dbl, StringType as Str, IntegerType as Int, LongType as Lng, DateType as Date, TimestampType as Time

def create_spark_session():
    """
    Create Spark session
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    spark.conf.set("mapreduce.fileoutputcommitter.algorithm.version", "2")
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Read song data files in JSON-format from Amazon S3,
    load the processed data into two analytical tables,
    and write these tables as parquet files back to Amazon S3.
    """
    # get filepath to song data file
    song_data = os.path.join(input_data, "song_data", *3*["*"], "*.json")
    #song_data = os.path.join("data", "song_data", *3*["*"], "*.json")
    
    # create song data schema
    song_data_schema = R([
        Fld("artist_id", Str(), False),
        Fld("artist_latitude", Str(), True),
        Fld("artist_longitude", Str(), True),
        Fld("artist_location", Str(), True),
        Fld("artist_name", Str(), False),
        Fld("song_id", Str(), False),
        Fld("title", Str(), False),
        Fld("duration", Dbl(), False),
        Fld("year", Int(), False)
    ])
    
    # read song data file
    df = spark.read.json(path=song_data, schema=song_data_schema)

    # extract columns to create songs table
    songs_table = df.select("song_id", "title", "artist_id", "year", "duration").distinct()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet(
        output_data + "songs_table.parquet",
        mode="overwrite",
        partitionBy=["year", "artist_id"]
    )

    # extract columns to create artists table
    artists_table = df.select("artist_id",
                              col("artist_name").alias("name"),
                              col("artist_location").alias("location"),
                              col("artist_latitude").alias("latitude"),
                              col("artist_longitude").alias("longitude")
                             ).distinct()

    # write artists table to parquet files
    artists_table.write.parquet(output_data + "artists_table.parquet", mode="overwrite")


def process_log_data(spark, input_data, output_data):
    """
    Read log data files in JSON-format from Amazon S3,
    load the processed data into three analytical tables,
    and write these tables as parquet files back to Amazon S3.
    """
    # get filepath to log data file
    log_data = os.path.join(input_data, "log_data", *2*["*"], "*.json")
    #log_data = os.path.join("data", "log_data", "*")

    # create log data schema
    log_data_schema = R([
        Fld("artist", Str(), True),
        Fld("auth", Str(), False),
        Fld("firstName", Str(), True),
        Fld("gender", Str(), True),
        Fld("itemInSession", Int(), False),
        Fld("lastName", Str(), True),
        Fld("length", Dbl(), True),
        Fld("level", Str(), False),
        Fld("location", Str(), True),
        Fld("method", Str(), False),
        Fld("page", Str(), False),
        Fld("registration", Dbl(), True),
        Fld("sessionId", Int(), False),
        Fld("song", Str(), True),
        Fld("status", Int(), False),
        Fld("ts", Lng(), False),
        Fld("userAgent", Str(), True),
        Fld("userId", Str(), True)
    ])
    
    # read log data file
    df = spark.read.json(path=log_data, schema=log_data_schema)
    
    # filter by actions for song plays
    df = df.filter(col("page") == "NextSong")

    # extract columns for users table    
    users_table = df.withColumn(
                                "max_ts_usr", max("ts").over(Window.partitionBy("userID"))
                   ).filter(
                            (col("ts") == col("max_ts_usr")) &
                            (col("userID").isNotNull()) &
                            (col("userID") != "")
                   ).select(
                            col("userID").alias("user_id"),
                            col("firstName").alias("first_name"),
                            col("lastName").alias("last_name"),
                            "gender",
                            "level"
                   ).distinct()
    
    # write users table to parquet files
    users_table.write.parquet(output_data + "users_table.parquet",
                              mode="overwrite",
                              partitionBy=["gender", "level"])

    # create timestamp column from original timestamp column
    #get_timestamp = udf()
    #df = df.withColumn("ts", to_timestamp("ts"))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp(x / 1000).replace(microsecond=0), Time())
    df = df.withColumn("start_time", get_datetime("ts"))
    
    # extract columns to create time table
    time_table = df.withColumn("hour", hour("start_time")) \
                   .withColumn("day", dayofmonth("start_time")) \
                   .withColumn("week", weekofyear("start_time")) \
                   .withColumn("month", month("start_time")) \
                   .withColumn("year", year("start_time")) \
                   .withColumn("weekday", dayofweek("start_time")) \
                   .select("start_time", "hour", "day", "week", "month", "year", "weekday") \
                   .distinct()
    
    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(output_data + "time_table.parquet",
                             mode="overwrite",
                             partitionBy=["year", "month"])

    # read song data to use for songplays table
    songs_table = spark.read.parquet(output_data + "songs_table.parquet")
    
    # read artists data to use for songplays table
    artists_table = spark.read.parquet(output_data + "artists_table.parquet")

    # extract columns from joined song and log datasets to create songplays table 
    songs = songs_table.join(artists_table, "artist_id", how="left") \
                   .select("song_id", "title", "artist_id", "name", "duration")
    
    songplays_table = df.join(songs, [df.artist == songs.name,
                                      df.song == songs.title,
                                      df.length == songs.duration], how="inner")
    
    songplays_table = songplays_table.join(time_table, "start_time", "left") \
                    .select("start_time",
                            col("userId").alias("user_id"),
                            "level",
                            "song_id",
                            "artist_id",
                            col("sessionId").alias("session_id"),
                            "location",
                            col("userAgent").alias("user_agent"),
                            "year",
                            "month"
                           ) \
                    .withColumn("songplay_id", monotonically_increasing_id())

    #write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet(
        output_data + "songplays_table.parquet",
        mode="overwrite",
        partitionBy=["year", "month"]
    )
    

def main():
    """
    1. Create an object of `ConfigParser` and read AWS-credentials and output bucket name into it.
    2. Create  an Amazon S3 bucket for output.
    3. Create a Spark session.
    4. Process song data files.
    5. Process log data files.
    """
    # create ConfigParser-object
    config = configparser.ConfigParser()
    config.read('dl.cfg')

    # read AWS-credentials and output bucket name
    os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
    os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']
    output_bucket = config['BUCKET']['BUCKET_OUTPUT_NAME']

    input_data = "s3a://udacity-dend/"
    #output_data = os.path.join("s3a://", output_bucket, "")
    #output_data = "s3a://" + output_bucket + "/"
    output_data = "s3a://{}/".format(output_bucket)
    
    try:
        # create output S3 bucket
        s3 = boto3.client('s3')
        s3.create_bucket(Bucket=output_bucket)
        
        # create Spark session
        spark = create_spark_session()
        if spark:
            # process song data files
            process_song_data(spark, input_data, output_data)
            # process log data files
            process_log_data(spark, input_data, output_data)
            
    except ParamValidationError as e:
        print(e)
    except ClientError as e:
        print(e)


if __name__ == "__main__":
    main()