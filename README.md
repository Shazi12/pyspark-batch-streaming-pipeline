# pyspark-batch-streaming-pipeline
### Overview

This project demonstrates the design of both batch and streaming data processing pipelines using Apache Spark. The pipelines ingest raw text data, perform distributed transformations, and generate aggregated datasets suitable for downstream analytics.

### Features

Batch processing pipeline for distributed word-frequency analysis
Real-time streaming pipeline using Spark Streaming
Automated dataset generation for pipeline testing
Tokenization, stop-word filtering, and aggregation transformations
Output datasets stored for downstream consumption

### Technologies

Python
Apache Spark (PySpark)
Spark Streaming

### Pipeline Architecture

Data ingestion from text files and streaming TCP source
Distributed processing using Spark RDD transformations
Aggregation and transformation of processed data
Storage of processed datasets for analysis

### Example Use Cases

Large-scale text analytics
Real-time streaming data aggregation
Distributed ETL experimentation
