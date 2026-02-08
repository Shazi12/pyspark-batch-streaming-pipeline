# Data Engineering Pipelines with PySpark

This repository contains a collection of batch, streaming, and ETL data pipelines built using PySpark.  
The goal of this project is to demonstrate core data engineering concepts including distributed processing, data cleaning, transformation, and dataset preparation for downstream analytics.

These projects reflect hands-on experience designing end-to-end pipelines from raw ingestion to structured outputs.

---

## 🚀 Project Overview

The repository includes three pipelines:

### 1. Batch Word Count Pipeline
**File:** `batch_wordcount.py`

A distributed batch processing pipeline that:
- Ingests multiple text files
- Performs tokenization and stop-word filtering
- Aggregates word frequencies using Spark RDD transformations
- Outputs sorted word counts for analysis

**Concepts demonstrated:**
- Distributed batch processing
- RDD transformations
- ETL-style workflow design
- Scalable aggregation

---

### 2. Streaming K-mer Pipeline
**File:** `streaming_kmer_pipeline.py`

A real-time streaming pipeline using Spark Streaming that:
- Ingests text data from a TCP socket
- Generates k-mers from incoming records
- Performs real-time aggregation
- Writes streaming outputs to disk for monitoring and analysis

**Concepts demonstrated:**
- Streaming ingestion
- Stateful transformations
- Real-time aggregation
- Streaming sinks
- Pipeline observability

---

### 3. Artist Popularity ETL Pipeline
**File:** `artist_popularity_pipeline.py`

A Spark SQL ETL pipeline for cleaning and preparing music streaming datasets across multiple platforms.

The pipeline:
- Loads raw CSV datasets
- Removes duplicates and excessive null values
- Standardizes schemas and data types
- Performs data validation and imputation
- Outputs cleaned datasets in Parquet format for downstream analytics

**Concepts demonstrated:**
- Spark DataFrame-based ETL
- Schema standardization
- Data quality checks
- Median imputation
- Analytical dataset preparation
- Parquet-based storage

---

## 🛠 Technologies Used

- Python
- PySpark
- Spark Streaming
- Spark SQL / DataFrames
- Parquet

---

## 📐 Pipeline Architecture

Each pipeline follows a common pattern:

1. Data ingestion (files or streaming sources)
2. Distributed transformation and cleaning
3. Aggregation and validation
4. Structured output for downstream consumption

This mirrors production-style data engineering workflows.

---

## 🎯 Purpose

This project was created to build practical experience with data engineering fundamentals including:

- Batch and streaming pipelines
- Distributed data processing
- ETL design
- Dataset preparation
- Data quality handling

It represents a transition from academic coursework to production-style data engineering systems.

---

## ▶️ Running the Pipelines

### Batch Word Count
```bash
spark-submit batch_wordcount.py
```
### Streaming Pipeline (requires Netcat)
```bash
nc -lk 9999
spark-submit streaming_kmer_pipeline.py
```
### Artist Popularity ETL

```bash
spark-submit artist_popularity_pipeline.py

```
