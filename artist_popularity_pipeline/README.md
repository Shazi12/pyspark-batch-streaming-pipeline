# 3. Artist Popularity ETL Pipeline
**File:** `artist_popularity_pipeline.py`

A production-ready Spark SQL ETL pipeline for cleaning and preparing music streaming datasets across multiple platforms (Spotify, YouTube, TikTok, Apple Music, etc.).

**Background:**  
This pipeline was originally developed as part of a data analysis project to optimize artist selection for University at Buffalo's Spring Fest. The raw datasets contained significant data quality issues that needed to be addressed before performing downstream analytics to identify which artists would maximize event attendance and student engagement.

**The pipeline:**
- Loads raw CSV datasets (114,000 tracks + 4,600 streaming records)
- Removes duplicates and records with excessive null values (>30%)
- Converts data types with robust error handling using `try_cast`
- Handles integer overflow for large streaming counts (billions of streams)
- Performs median imputation for missing values in streaming metrics
- Standardizes text fields (artist names, track titles, genres)
- Validates data ranges (popularity scores 0-100, positive durations)
- Removes columns with >30% missing data
- Outputs cleaned datasets in Parquet format for analysis

**Data Quality Improvements:**
- **Before:** 114,000 rows with ~15% null values, mixed types, duplicates
- **After:** 98,000 clean rows with <2% null values, validated types, ready for analysis
- **Completeness:** 87% → 98%

**Key Engineering Decisions:**
- Used `try_cast` instead of `cast` to handle malformed CSV data without pipeline crashes
- Used BIGINT for streaming counts that exceed INT limits (popular songs have 4B+ streams)
- Implemented approximate median calculation for performance on large datasets
- Output to Parquet for columnar storage and efficient downstream querying

**Concepts demonstrated:**
- Spark DataFrame-based ETL
- Schema standardization and type safety
- Data quality validation
- Statistical imputation (median)
- Handling real-world messy data
- Parquet-based analytical storage

**Downstream Use:**  
The cleaned datasets enable analysis for:
- Correlation between social media engagement and popularity
- Genre-based popularity trends
- Cross-platform streaming patterns
- Artist recommendation systems
- Predictive modeling for event success

---

## 📊 Dataset Sources

**Artist Popularity Pipeline:**
1. [Most Streamed Spotify Songs 2024](https://www.kaggle.com/datasets/nelgiriyewithana/most-streamed-spotify-songs-2024) (Kaggle)
   - 4,600 records, 29 columns
   - Cross-platform streaming metrics

2. [Spotify Tracks Dataset](https://www.kaggle.com/datasets/maharshipandya/-spotify-tracks-dataset/data) (Kaggle)
   - 114,000 records, 21 columns
   - Track metadata and audio features

---

## 🛠 Technologies Used

- **Python 3.8+**
- **PySpark 4.0+**
- **Spark Streaming**
- **Spark SQL / DataFrames**
- **Parquet** for efficient columnar storage

---

## 📐 Pipeline Architecture

Each pipeline follows a common ETL pattern:

1. **Extract:** Data ingestion from files or streaming sources
2. **Transform:** Distributed cleaning, validation, and aggregation
3. **Load:** Structured output for downstream consumption

This mirrors production-style data engineering workflows used in industry.

---

## 🎯 Purpose

This project was created to build practical experience with data engineering fundamentals including:

- Batch and streaming data pipelines
- Distributed data processing at scale
- Production ETL design patterns
- Dataset preparation and quality assurance
- Handling real-world messy data

It represents a transition from academic coursework to production-style data engineering systems.

---

## ▶️ Running the Pipelines

### Prerequisites
```bash
# Install dependencies
pip install pyspark

# Verify installation
python -c "import pyspark; print(pyspark.__version__)"
```

### Batch Word Count
```bash
spark-submit batch_wordcount.py
```

### Streaming K-mer Pipeline
```bash
# Terminal 1: Start Netcat server
nc -lk 9999

# Terminal 2: Run pipeline
spark-submit streaming_kmer_pipeline.py
```

### Artist Popularity ETL
```bash
# Place CSV files in datasets/ directory
# - datasets/tracks.csv
# - datasets/most_streamed.csv

# Run pipeline
spark-submit artist_popularity_pipeline.py

# Output will be in output/ directory as Parquet files
```

---

## 📁 Project Structure

```
pyspark-pipelines/
│
├── batch_wordcount.py               # Batch processing pipeline
├── streaming_kmer_pipeline.py       # Real-time streaming pipeline
├── artist_popularity_pipeline.py    # ETL pipeline for music data
│
├── datasets/                        # Input data (not tracked in git)
│   ├── tracks.csv
│   └── most_streamed.csv
│
├── output/                          # Pipeline outputs
│   ├── cleaned_tracks/
│   └── cleaned_streams/
│
├── requirements.txt                 # Python dependencies
└── README.md
```

---

## 📈 Example: Exploratory Analysis Results

After running the Artist Popularity ETL pipeline, the cleaned data enabled several key insights:

### Distribution of Artist Popularity
<img src="docs/popularity_distribution.png" width="600">

*Large spike of low-popularity artists, with remaining data following a bell curve. Filtering for artists above a popularity threshold improves recommendation quality.*

### Spotify vs. YouTube Correlation
<img src="docs/spotify_youtube_correlation.png" width="600">

*Correlation coefficient: 0.51. Indicates need for scaling metrics across platforms, as different services have different user bases.*

### Genre Popularity Analysis
<img src="docs/genre_analysis.png" width="600">

*Pop, Hip-Hop, and Rock show highest average popularity scores. Genre selection directly impacts potential event attendance.*

### Engagement Across Platforms
<img src="docs/platform_engagement.png" width="600">

*Spotify dominates total streams. TikTok views less reliable (inflated by background music usage). Multi-platform analysis essential for accurate artist evaluation.*

### Key Findings:
- **Platform Scaling Required:** Raw stream counts vary significantly between platforms
- **Genre Matters:** Pop and Hip-Hop consistently show higher engagement
- **Recency Bias:** Older songs may have legacy popularity scores that don't reflect current trends
- **TikTok Caveat:** Views can be inflated by royalty-free background music (e.g., Kevin MacLeod)

These insights informed the downstream artist selection model for Spring Fest optimization.

---

## 🔍 Data Cleaning Details

The Artist Popularity pipeline implements comprehensive data quality checks:

| Cleaning Step | Purpose |
|--------------|---------|
| **Duplicate Removal** | Prevents repeated records from skewing analysis |
| **Null Threshold Filter** | Drops rows/columns with >30% missing data |
| **Type Conversion** | Ensures numeric columns are properly typed (INT, BIGINT, FLOAT) |
| **Median Imputation** | Fills missing streaming metrics with statistical estimates |
| **Text Standardization** | Normalizes artist/track names (lowercase, trimmed) |
| **Range Validation** | Filters invalid values (negative durations, popularity >100) |
| **Overflow Protection** | Uses BIGINT for large counts to prevent integer overflow |

---

## 🚧 Future Enhancements

- [ ] Add unit tests for data quality validation
- [ ] Implement incremental processing for new streaming data
- [ ] Add Delta Lake for versioned datasets
- [ ] Create automated data quality reporting
- [ ] Build artist recommendation model using cleaned data
- [ ] Add support for real-time streaming updates

---

## 📧 Contact

**Shanoya Henry**  
🔗 [LinkedIn](www.linkedin.com/in/shanoya-henry) | [GitHub](https://github.com/Shazi12)

*Looking for Summer 2026 internship opportunities in Data Engineering*

---

## 🙏 Acknowledgments

- University at Buffalo Spring Fest committee for project inspiration
- Kaggle for providing public music streaming datasets
- Apache Spark community for distributed computing framework