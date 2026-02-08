"""
Artist Popularity Analysis Pipeline
====================================
ETL pipeline for cleaning and analyzing music streaming data using PySpark.
Processes track metadata and streaming metrics from multiple platforms.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)


def run_artist_popularity_pipeline(track_path, stream_path, output_path):
    """
    Main ETL pipeline for artist popularity analysis.
    
    Steps:
    1. Load and validate data
    2. Clean and transform datasets
    3. Remove duplicates and handle nulls
    4. Standardize data types and text
    5. Save cleaned datasets
    
    Args:
        track_path (str): Path to tracks CSV file
        stream_path (str): Path to most streamed CSV file
        output_path (str): Directory for output files
    """
    logging.info("Starting Artist Popularity ETL Pipeline")
    
    # Initialize Spark
    spark = SparkSession.builder \
        .appName("ArtistPopularityPipeline") \
        .master("local[*]") \
        .getOrCreate()
    
    try:
        # ==================== EXTRACT ====================
        logging.info("Loading datasets...")
        track_data = spark.read.csv(track_path, header=True, inferSchema=True)
        most_streamed = spark.read.csv(stream_path, header=True, inferSchema=True)
        
        logging.info(f"Loaded {track_data.count()} tracks and {most_streamed.count()} streaming records")
        
        # ==================== TRANSFORM ====================
        logging.info("Starting data cleaning transformations...")
        
        # 1. Remove duplicates
        logging.info("Removing duplicates...")
        track_data = track_data.dropDuplicates()
        most_streamed = most_streamed.dropDuplicates()
        
        # 2. Remove rows with >30% null values
        logging.info("Filtering rows with excessive nulls...")
        track_threshold = int(len(track_data.columns) * 0.7)
        stream_threshold = int(len(most_streamed.columns) * 0.7)
        
        track_data = track_data.dropna(thresh=track_threshold)
        most_streamed = most_streamed.dropna(thresh=stream_threshold)
        
        # 3. Convert data types for track_data
        logging.info("Converting data types for track_data...")
        
        # Define all numeric columns for track_data
        track_float_cols = ['danceability', 'energy', 'key', 'loudness', 'speechiness', 
                           'acousticness', 'instrumentalness', 'liveness', 'valence', 
                           'tempo', 'time_signature', 'duration_ms']
        
        track_int_cols = ['mode', 'popularity']
        
        # Convert float columns with try_cast
        for col_name in track_float_cols:
            if col_name in track_data.columns:
                track_data = track_data.withColumn(
                    col_name,
                    when(col(col_name).isNotNull(),
                         regexp_replace(col(col_name).cast("string"), ",", ""))
                )
                track_data = track_data.withColumn(
                    col_name,
                    expr(f"try_cast(`{col_name}` as double)")
                )
        
        # Convert integer columns with try_cast
        for col_name in track_int_cols:
            if col_name in track_data.columns:
                track_data = track_data.withColumn(
                    col_name,
                    when(col(col_name).isNotNull(),
                         regexp_replace(col(col_name).cast("string"), ",", ""))
                )
                track_data = track_data.withColumn(
                    col_name,
                    expr(f"try_cast(`{col_name}` as int)")
                )
        
        # Convert explicit to boolean/int
        if 'explicit' in track_data.columns:
            track_data = track_data.withColumn(
                "explicit",
                when(lower(trim(col("explicit"))) == "true", 1)
                .when(lower(trim(col("explicit"))) == "false", 0)
                .otherwise(None)
            )
        
        # 4. Convert data types for most_streamed
        logging.info("Converting data types for most_streamed...")
        
        # Use BIGINT for large streaming numbers to avoid overflow
        stream_bigint_cols = ['Spotify Streams', 'Spotify Playlist Reach', 'YouTube Views',
                             'YouTube Likes', 'TikTok Posts', 'TikTok Likes', 'TikTok Views',
                             'YouTube Playlist Reach', 'Deezer Playlist Reach',
                             'Pandora Streams', 'Shazam Counts']
        
        # Use INT for smaller counts and rankings
        stream_int_cols = ['All Time Rank', 'Spotify Playlist Count', 'Spotify Popularity',
                          'Apple Music Playlist Count', 'AirPlay Spins', 'Deezer Playlist Count',
                          'Amazon Playlist Count', 'Pandora Track Stations', 'Explicit Track']
        
        # Convert BIGINT columns
        for col_name in stream_bigint_cols:
            if col_name in most_streamed.columns:
                most_streamed = most_streamed.withColumn(
                    col_name,
                    when(col(col_name).isNotNull(),
                         regexp_replace(col(col_name).cast("string"), ",", ""))
                )
                most_streamed = most_streamed.withColumn(
                    col_name,
                    expr(f"try_cast(`{col_name}` as bigint)")
                )
        
        # Convert INT columns
        for col_name in stream_int_cols:
            if col_name in most_streamed.columns:
                most_streamed = most_streamed.withColumn(
                    col_name,
                    when(col(col_name).isNotNull(),
                         regexp_replace(col(col_name).cast("string"), ",", ""))
                )
                most_streamed = most_streamed.withColumn(
                    col_name,
                    expr(f"try_cast(`{col_name}` as int)")
                )
        
        # 5. Impute missing values with median for streaming data
        logging.info("Imputing missing values...")
        
        # Combine both lists for imputation
        impute_cols = stream_bigint_cols + [col for col in stream_int_cols 
                                           if col not in ['All Time Rank', 'Explicit Track']]
        
        for col_name in impute_cols:
            if col_name not in most_streamed.columns:
                continue
                
            try:
                logging.info(f"Computing median for column: {col_name}")
                
                # Ensure column is numeric (double for median calculation)
                most_streamed = most_streamed.withColumn(
                    col_name, 
                    col(col_name).cast("double")
                )
                
                # Check if column has any non-null values
                non_null_count = most_streamed.filter(col(col_name).isNotNull()).count()
                
                if non_null_count == 0:
                    logging.warning(f"Skipping {col_name}: no non-null values")
                    continue
                
                # Compute median
                median_val = most_streamed.approxQuantile(col_name, [0.5], 0.01)[0]
                
                # Impute missing values
                most_streamed = most_streamed.withColumn(
                    col_name,
                    when(col(col_name).isNull(), median_val).otherwise(col(col_name))
                )
                
                logging.info(f"Imputed {col_name} with median: {median_val}")
                
            except Exception as e:
                logging.warning(f"Skipping median imputation for column '{col_name}': {e}")
                continue
        
        # 6. Standardize text columns
        logging.info("Standardizing text columns...")
        
        track_text_cols = ['artists', 'album_name', 'track_name', 'track_genre']
        for col_name in track_text_cols:
            if col_name in track_data.columns:
                track_data = track_data.withColumn(
                    col_name, 
                    trim(lower(col(col_name)))
                )
        
        stream_text_cols = ['Track', 'Album Name', 'Artist']
        for col_name in stream_text_cols:
            if col_name in most_streamed.columns:
                most_streamed = most_streamed.withColumn(
                    col_name, 
                    trim(lower(col(col_name)))
                )
        
        # 7. Validate and filter invalid data
        logging.info("Filtering invalid records...")
        
        if 'duration_ms' in track_data.columns and 'popularity' in track_data.columns:
            initial_count = track_data.count()
            track_data = track_data.filter(
                (col("duration_ms").isNotNull()) &
                (col("duration_ms") > 0) & 
                (col("popularity").isNotNull()) &
                (col("popularity") >= 0) & 
                (col("popularity") <= 100)
            )
            filtered_count = track_data.count()
            logging.info(f"Filtered {initial_count - filtered_count} invalid track records")
        
        if 'Spotify Popularity' in most_streamed.columns:
            initial_count = most_streamed.count()
            most_streamed = most_streamed.filter(
                (col("Spotify Popularity").isNotNull()) &
                (col("Spotify Popularity") >= 0) & 
                (col("Spotify Popularity") <= 100)
            )
            filtered_count = most_streamed.count()
            logging.info(f"Filtered {initial_count - filtered_count} invalid streaming records")
        
        # 8. Remove columns with >30% null values
        logging.info("Removing columns with excessive nulls...")
        
        # For track_data
        total_rows = track_data.count()
        
        if total_rows > 0:
            nonnull_counts = track_data.select([
                count(when(col(c).isNotNull(), c)).alias(c) for c in track_data.columns
            ]).collect()[0].asDict()
            
            track_drop_cols = [c for c, nn in nonnull_counts.items() if nn < total_rows * 0.7]
            
            if track_drop_cols:
                track_data = track_data.drop(*track_drop_cols)
                logging.info(f"Dropped {len(track_drop_cols)} track_data columns: {track_drop_cols}")
            else:
                logging.info("No track_data columns dropped due to null threshold")
        
        # For most_streamed
        total_rows_stream = most_streamed.count()
        
        if total_rows_stream > 0:
            nonnull_counts_stream = most_streamed.select([
                count(when(col(c).isNotNull(), c)).alias(c) for c in most_streamed.columns
            ]).collect()[0].asDict()
            
            stream_drop_cols = [c for c, nn in nonnull_counts_stream.items() 
                               if nn < total_rows_stream * 0.7]
            
            if stream_drop_cols:
                most_streamed = most_streamed.drop(*stream_drop_cols)
                logging.info(f"Dropped {len(stream_drop_cols)} most_streamed columns: {stream_drop_cols}")
            else:
                logging.info("No most_streamed columns dropped due to null threshold")
        
        # ==================== LOAD ====================
        logging.info("Saving cleaned datasets...")
        
        # Show final schemas
        logging.info("Final track_data schema:")
        track_data.printSchema()
        
        logging.info("Final most_streamed schema:")
        most_streamed.printSchema()
        
        # Save outputs
        track_data.write.mode("overwrite").parquet(f"{output_path}/cleaned_tracks")
        most_streamed.write.mode("overwrite").parquet(f"{output_path}/cleaned_streams")
        
        # Log summary statistics
        logging.info(f"Final track_data: {track_data.count()} rows, {len(track_data.columns)} columns")
        logging.info(f"Final most_streamed: {most_streamed.count()} rows, {len(most_streamed.columns)} columns")
        
        logging.info(f"Pipeline completed successfully! Output saved to {output_path}")
        
    except Exception as e:
        logging.error(f"Pipeline failed: {str(e)}")
        raise
    
    finally:
        spark.stop()
        logging.info("Spark session stopped")


if __name__ == "__main__":
    # Run the pipeline
    run_artist_popularity_pipeline(
        track_path="datasets/tracks.csv",
        stream_path="datasets/most_streamed.csv",
        output_path="output"
    )