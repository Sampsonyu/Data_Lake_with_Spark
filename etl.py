import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
import pyspark.sql.types as T           # Used to define Schema struct


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = os.path.join(input_data, 'song_data/*/*/*/*.json')

    # read song data file
    df = spark.read.json(song_data, schema=song_schema)

    # extract columns to create songs table
    songs_table = df.select('song_id', 'title', 'artist_id', 'year', 'duration')

    # write songs table to parquet files partitioned by year and artist
    song_out_path = os.path.join(output_data, 'sparkify_songs_table/')
    songs_table.write.parquet(song_out_path, mode='overwrite', partitionBy=('year','id'))

    # extract columns to create artists table
    artists_table = df.select('artist_id', \
                              col('artist_name').alias('name'), \
                              col('artist_location').alias('location'),\
                              col('artist_latitude').alias('latitude'),\
                              col('artist_longitude').alias('longitude'))\
                              .dropDuplicates()

    # write artists table to parquet files
    artist_out_path = os.path.join(output_data, 'sparkify_artist_table/')
    artists_table.write.parquet(artist_out_path, mode='overwrite')


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file

    log_data = os.path.join(input_data, 'log_data/*/*/*.json')

    # read log data file
    df = spark.read.json(log_data, schema=log_schema)

    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # extract columns for users table
    users_table = df.repartition(df.user_id) \
                    .select(col('userId').alias('user_id'), \
                            col('firstName').alias('first_name'),\
                            col('lastName').alias('last_name'), \
                            'gender',
                            'level') \
                    .where(col("user_id").isNotNull()) \
                    .dropDuplicates()

    # write users table to parquet files
    user_out_path = os.path.join(output_data, 'sparkify_user_table/')
    users_table.write.parquet(user_out_path, mode="overwrite")

    # Defining User Defined Function (UDF) to convert log timestamp (seconds since epoch) to \
    # actual Datetime Type timestamp
    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x : datetime.utcfromtimestamp(int(x)/1000), T.TimestampType())
    df = df.withColumn('timestamp', get_timestamp(df.ts))

    # create datetime column from original timestamp column
    get_datetime = udf(lambda x : datetime.utcfromtimestamp(int(x)/1000), T.DateType())
    df = df.withColumn("datetime", get_timestamp("ts"))

    # extract columns to create time table
    time_table = df.select(col('timestamp').alias('start_time') \
                      , hour('timestamp').alias('hour') \
                      , dayofmonth('timestamp').alias('day') \
                      , weekofyear('timestamp').alias('week') \
                      , month('timestamp').alias('month') \
                      , year('timestamp').alias('year') \
                      , dayofweek('timestamp').alias('weekday'))

    # write time table to parquet files partitioned by year and month
    time_out_path = os.path.join(output_data, 'sparkify_time_table/')
    time_table.write.parquet(time_out_path, mode='overwrite', partitionBy=('year', 'month'))

    # read in song data to use for songplays table
    song_data = os.path.join(input_data, 'song_data/*/*/*/*.json')
    song_df = spark.read.json(song_data, schema=song_schema)
    # extract columns from joined song and log datasets to create songplays table
    df_song_log = df.join(song_df, [df.song == song_df.title,\
                                        df.artist == song_df.name])
    songplays_table = df_song_log.select('start_time',\
                                        col('userId').alias('user_id'),\
                                        'level',\
                                        'song_id',\
                                        'artist_id',\
                                        col('sessionId').alias('session_id'),\
                                        'location',\
                                        col('userAgent').alias('user_agent'), \
                                        'year', \
                                        'month')
    # write songplays table to parquet files partitioned by year and month
    songplays_out_path = os.path.join(output_data, 'sparkify_songplays_table/')
    songplays_table.write.parquet(songplays_out_path, mode='overwrite', partitionBy=('year', 'month'))


def main():
    '''
    Description: Call the create_spark_session function,
    process_song_data and process_log_data functions to create sparkify data lake
    '''

    spark = create_spark_session()

    # Songs table schema for JSON read
    song_schema = T.StructType([T.StructField('artist_id'       , T.StringType())
                             , T.StructField('artist_latitude'  , T.StringType())
                             , T.StructField('artist_location'  , T.StringType())
                             , T.StructField('artist_longitude' , T.StringType())
                             , T.StructField('artist_name'      , T.StringType())
                             , T.StructField('duration'         , T.DoubleType())
                             , T.StructField('num_songs'        , T.IntegerType())
                             , T.StructField('song_id'          , T.StringType())
                             , T.StructField('title'            , T.StringType())
                             , T.StructField('year'             , T.IntegerType())
                              ])
    # Log table schema for JSON read
    log_schema = T.StructType([ T.StructField('artist'          , T.StringType())
                              , T.StructField('auth'            , T.StringType())
                              , T.StructField('gender'          , T.StringType())
                              , T.StructField('lastName'        , T.StringType())
                              , T.StructField('length'          , T.DoubleType())
                              , T.StructField('level'           , T.StringType())
                              , T.StructField('firstName'       , T.StringType())
                              , T.StructField('itemInSession'   , T.IntegerType())
                              , T.StructField('location'        , T.StringType())
                              , T.StructField('method'          , T.StringType())
                              , T.StructField('page'            , T.StringType())
                              , T.StructField('registration'    , T.DoubleType())
                              , T.StructField('sessionId'       , T.IntegerType())
                              , T.StructField('song'            , T.StringType())
                              , T.StructField('status'          , T.IntegerType())
                              , T.StructField('ts'              , T.LongType())
                              , T.StructField('userAgent'       , T.StringType())
                              , T.StructField('userId'          , T.StringType())
                              ])
    # specify input data folder
    input_data = config['S3']['SPARKIFY_DATALAKE_INPUT_DATA_PATH']

    # specify output data folder
    output_data = config['S3']['SPARKIFY_DATALAKE_OUTPUT_DATA_PATH']

    # process song data
    process_song_data(spark, input_data, output_data)
    # process song data
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
