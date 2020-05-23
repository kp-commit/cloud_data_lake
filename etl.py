import datetime
import config as c
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, monotonically_increasing_id
from pyspark.sql.functions import year, month, hour, weekofyear, dayofmonth, dayofweek
from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import StringType, TimestampType, IntegerType, FloatType


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data):
    
    #----------------------------- SONG DATSET -------------------------------#
    print('PROCESSING SONG DATASET:')
    
    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*/*.json'    
    

    # read song data file
    songSchema = StructType([
                  StructField("num_songs", IntegerType()),
                  StructField("artist_id", StringType()),
                  StructField("artist_latitude", FloatType()),
                  StructField("artist_longitude", FloatType()),
                  StructField("artist_location", StringType()),
                  StructField("artist_name", StringType()),
                  StructField("song_id", StringType()),
                  StructField("title", StringType()),
                  StructField("duration", StringType()),
                  StructField("year", StringType())            
    ])
    dfSongsData = spark.read.json(song_data, schema=songSchema)
    print(' SONG DATASET READ')
    
    
    #---------------------------- SONGS TABLE --------------------------------#
    # extract columns to create songs table
    print(' PROCESSING SONGS TABLE')
    dfSongsTbl = dfSongsData.select(['song_id','title','artist_id','year','duration']) \
                    .dropDuplicates()
                    
    # create a temp view for songs table (used later for SongPlays table & counts)
    print(' CREATING TEMP VIEW SONGS')
    dfSongsTbl.createOrReplaceTempView('songs') 
    

    
    
    #--------------------------- ARTISTS TABLE -------------------------------#
    # extract columns to create artists table
    print(' PROCESSING ARTISTS TABLE')
    dfArtistsTbl = dfSongsData.select(['artist_id','artist_name','artist_location', \
                                      'artist_latitude','artist_longitude']) \
                        .withColumnRenamed('artist_name','name') \
                        .withColumnRenamed('artist_location','location') \
                        .withColumnRenamed('artist_latitude','latitude') \
                        .withColumnRenamed('artist_longitude','longitude') \
                        .dropDuplicates()
    
    # create a temp view for artists table (used later for SongPlays table & counts)
    print(' CREATING TEMP VIEW ARITSTS')
    dfArtistsTbl.createOrReplaceTempView('artists')
    
    
    return dfSongsTbl, dfArtistsTbl        
    #------------------------ end of song data process -----------------------#
    
    
    
def write_song_artists_s3(spark, output_data, dfSongsTbl, dfArtistsTbl):
    """
    Function to do writes for Artists and Songs Tables to S3 in Parquet format
    """    
    #---------------------- start songs & artists table writes ---------------#
    try:
        # write songs table to parquet files partitioned by year and artist
        dfSongsTbl.write.mode("overwrite").partitionBy("year", "artist_id").parquet(output_data + 'songs/')
        print(' SONGS TABLE WRITTEN TO {}'.format(output_data + 'songs/'))
    except Exception as e:
        print(e)
        
    try:        
        # write artists table to parquet files
        dfArtistsTbl.write.mode("overwrite").parquet(output_data + 'artists/')
        print(' ARTISTS TABLE WRITTEN TO {}'.format(output_data + 'artists/'))
    except Exception as e:
        print(e) 
    pass
    #---------------------- end songs & artists table writes -----------------#



def process_log_data(spark, input_data):
    #---------------------------- LOG DATASET --------------------------------#
    print('\nPROCESSING LOG DATASET:')    

    # get filepath to log data file
    log_data = input_data + 'log_data/*/*/*.json'
    

    # read log data file
    logSchema = StructType([
                  StructField("artist", StringType()),
                  StructField("auth", StringType()),
                  StructField("firstName", StringType()),
                  StructField("gender", StringType()),
                  StructField("itemInSession", IntegerType()),
                  StructField("lastName", StringType()),
                  StructField("length", FloatType()),
                  StructField("level", StringType()),
                  StructField("location", StringType()),
                  StructField("method", StringType()),
                  StructField("page", StringType()),
                  StructField("registration", FloatType()),
                  StructField("sessionId", IntegerType()),
                  StructField("song", StringType()),
                  StructField("status", IntegerType()),
                  StructField("ts", StringType()),
                  StructField("userAgent", StringType()),
                  StructField("userId", StringType())   
    ])
    dfLogData = spark.read.json(log_data, schema=logSchema)
    print(' LOG DATASET READ')
    
    # filter by actions for song plays
    dfLogData = dfLogData.filter(dfLogData.page == 'NextSong') 
    print(' LOG DATASET FILTERED ON NextSong')

    
    #---------------------------- USERS TABLE --------------------------------#    
    # extract columns for users table
    print(' PROCESSING USERS TABLE')    
    dfUserTbl = dfLogData.select(['userId','firstName','lastName','gender','level']) \
                        .withColumnRenamed('userId','user_id') \
                        .withColumnRenamed('firstName','first_name') \
                        .withColumnRenamed('lastName','last_name') \
                        .dropDuplicates()
                        
    # create a temp view for users table (used later for SongPlays table & counts)
    print(' CREATING TEMP VIEW USERS')
    dfUserTbl.createOrReplaceTempView('users')    
    

    #---------------------------- TIME TABLE ---------------------------------#    
    print(' PROCESSING TIME TABLE')
    # UDF for extracting timestamp:
    AsTimeStamp = udf(lambda x: datetime.datetime.fromtimestamp(float(x)/1000.0), TimestampType())
    
    #create datetime ('start_time') column from original timestamp column 
    dfTimeTbl = dfLogData.select('ts') \
                .withColumn('start_time', AsTimeStamp('ts')) \
                .dropDuplicates()                  
    
    # create datetime columns from original timestamp column
    dfTimeTbl = dfTimeTbl.withColumn('hour', hour('start_time')) \
                    .withColumn('day', dayofmonth('start_time')) \
                    .withColumn('week', weekofyear('start_time')) \
                    .withColumn('month', month('start_time')) \
                    .withColumn('year', year('start_time')) \
                    .withColumn('weekday', dayofweek('start_time'))
                    
    # create a temp view for time table (used later for SongPlays table & counts)
    print(' CREATING TEMP VIEW TIME')
    dfTimeTbl.createOrReplaceTempView('time')
    
    
    
    #---------------------------- SONGPLAYS TABLE ----------------------------# 
    
    # read in log data to use for songplays table
    # join with previous views created for tables: songs, artists, users, time 
    # create view for dfLogsData and join to create songplays table 
    dfLogData.createOrReplaceTempView('log_data')   
    
   
    # extract columns from joined song and log datasets to create songplays table 
    # utilize previous views, join, create songplays table
    print(' PROCESSING SONGPLAYS TABLE')
    dfSongPlaysTbl = spark.sql("""
        SELECT  t.start_time,
        		l.userId as user_id,
        		l.level,
        		s.song_id,
        		s.artist_id,
        		l.sessionId as session_id, 
        		l.location,
        		l.userAgent as user_agent
    	FROM log_data l
        JOIN time t
        ON l.ts = t.ts
    	JOIN songs s
    	ON l.song = s.title
        JOIN artists a
    	ON l.artist = a.name
    """)
    
    # Add an unique identifer in songplay table
    dfSongPlaysTbl = dfSongPlaysTbl.withColumn('songplay_id', monotonically_increasing_id())
    print(' COLUMN songplay_id ADDED')
    
    # create a temp view for songplay table (used later for table counts)
    dfSongPlaysTbl.createOrReplaceTempView('song_plays')
    
    return dfUserTbl, dfTimeTbl, dfSongPlaysTbl
    #------------------------- end of log data process -----------------------#
    

def write_user_time_songplays_s3(spark, output_data, dfUserTbl, dfTimeTbl, dfSongPlaysTbl):
    """
    Function to do writes for Users, Time and SongPlays Tables to S3 in Parquet format
    """
    #------------------ start users, time & songplays table writes -----------#
    try:
        # write users table to parquet files
        dfUserTbl.write.mode("overwrite").parquet(output_data + 'users/')
        print('\nTABLE USERS WRITTEN TO {}'.format(output_data + 'users/'))
    except Exception as e:
        print(e)
    
    try:
        # write time table to parquet files partitioned by year and month
        dfTimeTbl.write.mode("overwrite").partitionBy("year","month").parquet(output_data + 'time/')
        print('TABLE TIME WRITTEN TO {}'.format(output_data + 'time/'))    
    except Exception as e:
        print(e)
    
    try:
        # write songplays table to parquet files partitioned by year and month
        dfSongPlaysTbl.write.mode("overwrite").parquet(output_data + 'song_plays/')
        print('TABLE SONGPLAYS WRITTEN TO {}'.format(output_data + 'song_plays/'))
    except Exception as e:
        print(e) 
    pass
    #------------------- end users, time & songplays table writes ------------#


    
def tablecounts(spark, output_data): 
    """
    Function to do counts of all tables and write results as csv in S3
    """
    #------------------------- start of table counts -------------------------# 
    try:
        # count rows in songs
        print('\nGETTING ALL TABLE COUNTS') 
        AllTblCnt = spark.sql("""
            SELECT 'Songs:', COUNT(*) FROM songs
            UNION ALL
            SELECT 'Users:', COUNT(*) FROM users
            UNION ALL
            SELECT 'Time:', COUNT(*) FROM time
            UNION ALL
            SELECT 'SongsPlays:', COUNT(*) FROM song_plays
            UNION ALL
            SELECT 'Artists:', COUNT(*) FROM artists    
        """)
        # write results to file
        AllTblCnt.coalesce(1).write.mode('overwrite').options(header="false").csv(output_data + 'AllTblCnt')
        print(' GOT ALL TABLE COUNTS, WRITTEN TO {}'.format(output_data + 'AllTblCnt/'))
    except Exception as e:
        print(e) 
    
    pass    
    #------------------------- end of table counts ---------------------------#
    

def run_analytics(spark, output_data):
    """
    Function to do run analytical queries and write result as csv in S3
    """
    #------------------------- start analytics -------------------------------# 
    try:
        # Top 10 Songs
        print('GETTING TOP SONGS') 
        top_songs = spark.sql("""
            WITH topsid as (
            	SELECT song_id, count(*) as count
            	FROM song_plays
            	GROUP BY song_id
            	ORDER BY 2 DESC
            	LIMIT 10
            )
            SELECT '"'|| dms.title ||'" by '|| dma.name || ' played ' || ft.count || ' times.'
            FROM topsid ft
            INNER JOIN songs dms 
            ON ft.song_id = dms.song_id
            INNER JOIN artists dma
            ON dms.artist_id = dma.artist_id
            GROUP BY dms.title, dma.name, ft.count
            ORDER BY ft.count DESC    
        """)
        # write results to file
        top_songs.coalesce(1).write.mode('overwrite').options(header="true").csv(output_data + 'top_songs')
        print('Top Songs Result, WRITTEN TO {}'.format(output_data + 'top_songs/'))    
    except Exception as e:
        print(e)

    try:
        # Top 10 Artists
        print('GETTING TOP ARTISTS') 
        top_artists = spark.sql("""
            WITH topart as (
            	SELECT artist_id, song_id, count(*) as count
            	FROM song_plays
            	GROUP BY artist_id, song_id
            	ORDER BY 2 DESC
            	LIMIT 10
            )
            SELECT '"'||dma.name||'" was played '||ft.count||' times.'
            FROM topart ft
            INNER JOIN artists dma 
            ON ft.artist_id = dma.artist_id
            INNER JOIN songs dms 
            ON ft.song_id = dms.song_id
            GROUP BY dma.name, ft.count
            ORDER BY ft.count DESC   
        """)
        # write results to file
        top_artists.coalesce(1).write.mode('overwrite').options(header="true").csv(output_data + 'top_artists')
        print('Top Songs Result, WRITTEN TO {}'.format(output_data + 'top_artists/'))    
    except Exception as e:
        print(e) 

    try:
        # Paid versus Free Ratio
        print('GETTING ALL TABLE COUNTS') 
        paid_free_rt = spark.sql("""
            SELECT 'Ratio: ', 'Free users: '||f.free, 'Paid users: '||p.paid, (f.free/p.paid)||':'||(p.paid/p.paid)
            FROM (
                SELECT count(user_id) as paid 
                FROM users
                WHERE level = 'paid'
            ) p CROSS JOIN (
                SELECT count(user_id) as free
                FROM users
                WHERE level = 'free'
            ) f    
        """)
        # write results to file
        paid_free_rt.coalesce(1).write.mode('overwrite').options(header="true").csv(output_data + 'paid_free_rt')
        print('Paid versus Free Ratio, WRITTEN TO {}'.format(output_data + 'paid_free_rt/'))    
    except Exception as e:
        print(e)
    #--------------------------- end analytics -------------------------------#    
    pass 
    
    
  

def main():
    spark = create_spark_session()
    input_data = c.INPUT_BUCKET
    output_data = c.OUTPUT_BUCKET
    
    dfSongsTbl, dfArtistsTbl = process_song_data(spark, input_data)    
    dfUserTbl, dfTimeTbl, dfSongPlaysTbl = process_log_data(spark, input_data)
    
    write_song_artists_s3(spark, output_data, dfSongsTbl, dfArtistsTbl)
    write_user_time_songplays_s3(spark, output_data, dfUserTbl, dfTimeTbl, dfSongPlaysTbl)
    
    tablecounts(spark, output_data)
    run_analytics(spark, output_data)


if __name__ == "__main__":
    main()
