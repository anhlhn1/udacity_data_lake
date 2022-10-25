import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, dayofweek, date_format
from pyspark.sql.types import *
import pyspark.sql.types as T

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    '''
    This function is used to initialize SparkSession that will be used for the rest of application
    '''
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    '''
    This function is used to load all raw data from the bucket input data in S3
    then transform to get song_table + artist_table
    Finally, we will write song_table + artist_table in partition to store in S3 again.
    By this way, we are constructing S3 as a Data Lake, which stores both raw and cleansed data
    Parameters:
    - spark: SparkSession
    - input_data: the S3 input path where has raw data
    - output_data: the S3 output path where you store your transformed tables 
    '''
    # get filepath to song data file
    song_data = input_data + '/song_data/*/*/*/*'
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select("song_id", "artist_id", "title", "year", "duration").dropDuplicates()

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id").mode("overwrite").save(output_data + "/song_data/song_table.parquet")

    # extract columns to create artists table
    artists_table = df.select("artist_id", "artist_latitude", "artist_location", "artist_longitude", "artist_name").dropDuplicates()
    
    # write artists table to parquet files
    artists_table.write.mode("overwrite").save(output_data + "/artist_data/artist_table.parquet")


def process_log_data(spark, input_data, output_data):
    '''
    This function is used to load all raw data from the bucket input data in S3
    then transform to get user_tables + time_table + songplays_table 
    Finally, we will write transformed tables in partition to store in S3 again.
    By this way, we are constructing S3 as a Data Lake, which stores both raw and cleansed data
    Parameters:
    - spark: SparkSession
    - input_data: the S3 input path where has raw data
    - output_data: the S3 output path where you store your transformed tables 
    '''
    # get filepath to log data file
    log_data = input_data + '/log_data/*/*/*'

    # read log data file
    log_schema = T.StructType([
      T.StructField('artist', T.StringType(),True),
      T.StructField('auth',T.StringType(),True),
      T.StructField('firstName',T.StringType(),True),
      T.StructField('gender',T.StringType(),True),
      T.StructField('itemInSession',T.IntegerType(),True),
      T.StructField('lastName',T.StringType(),True),
      T.StructField('length',T.FloatType(),True),
      T.StructField('level',T.StringType(),True),
      T.StructField('location',T.StringType(),True),
      T.StructField('method',T.StringType(),True),
      T.StructField('page',T.StringType(),True),
      T.StructField('registration',T.FloatType(),True),
      T.StructField('sessionId',T.IntegerType(),True),
      T.StructField('song',T.StringType(),True),
      T.StructField('status',T.IntegerType(),True),
      T.StructField('ts',T.DoubleType(),True),
      T.StructField('userAgent',T.StringType(),True),
      T.StructField('userId',T.StringType(),True),
    ])
    df = spark.read.option("recursiveFileLookup","true").schema(log_schema).json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == "NextSong")
    
    # extract columns for users table    
    users_table = df.select("userId", "firstName", "lastName", "gender", "level").dropDuplicates()
    
    # write users table to parquet files
    users_table.write.mode("overwrite").parquet(output_data + "/user_data/user_table.parquet")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp(int(x)/1000), TimestampType())
    df = df.withColumn("start_time", get_timestamp("ts"))

    # extract columns to create time table
    time_table = df.select("start_time", 
                           hour("start_time").alias('hour'), \
                           dayofmonth("start_time").alias('day'), \
                           weekofyear("start_time").alias('week'), \
                           month("start_time").alias('month'), \
                           year("start_time").alias('year'), \
                           dayofweek("start_time").alias('weekday').dropDuplicates()
    )

    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year", "month").mode("overwrite").parquet(output_data + "/time_data/time_table.parquet")

    # read in song data to use for songplays table
    song_df = spark.read.parquet(output_data + "/song_data/song_table.parquet")
    artist_df = spark.read.parquet(output_data + "/artist_data/artist_table.parquet")

    # extract columns from joined song and log datasets to create songplays table 
    song_df = song_df.withColumnRenamed("artist_id", "s_artist_id")
    songplays_table = song_df.join(artist_df, song_df.s_artist_id == artist_df.artist_id, "inner")
    songplays_table = songplays_table.join(df, (songplays_table.title == df.song) & \
                                           (songplays_table.artist_name == df.artist), "inner")
    songplays_table = songplays_table.select(get_timestamp("ts").alias("start_time"), \
                                             year("start_time").alias('year'), \
                                             month("start_time").alias('month'), \
                                             "userId", "level", "song_id", \
                                            "artist_id", "sessionId", "location", "userAgent").dropDuplicates()

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year", "month").mode("overwrite")\
                        .parquet(output_data + "/songplays_data/songplays_table.parquet")

    

def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3://anhlhn/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
