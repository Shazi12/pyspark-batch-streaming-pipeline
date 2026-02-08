## Streaming K-mer Pipeline

### Overview

This pipeline demonstrates real-time data ingestion and transformation using Spark Streaming. Incoming text data is processed as a continuous stream, converted into k-mers, and aggregated in near real time.

The pipeline models streaming ingestion scenarios such as log processing or real-time analytics.

---

### Data Flow

Input:
- TCP socket stream (Netcat)

Processing:
1. Real-time ingestion via Spark Streaming
2. Generation of k-mer substrings from incoming records
3. Mapping of k-mers to `(kmer, 1)` pairs
4. Aggregation using `reduceByKey`
5. Continuous writing of results to disk

Output:
- Aggregated k-mer counts saved incrementally

---

### Key Transformations

- `flatMap()` for k-mer generation
- `map()` for key-value creation
- `reduceByKey()` for streaming aggregation
- `foreachRDD()` for custom sink handling

---

### Engineering Concepts Demonstrated

- Streaming ingestion
- Micro-batch processing
- Stateful transformations
- Custom output sinks
- Pipeline monitoring using console output

---

### Use Case

This pipeline simulates real-time text analytics workloads such as event stream processing, monitoring pipelines, or live data aggregation systems.
