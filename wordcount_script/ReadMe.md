## Batch Word Count Pipeline

### Overview

This pipeline demonstrates distributed batch processing using PySpark RDDs. The goal is to ingest multiple raw text files, perform tokenization and filtering, and generate aggregated word frequency statistics at scale.

The pipeline simulates a classic ETL batch workload where unstructured text data is transformed into structured analytical output.

---

### Data Flow

Input:
- Multiple raw text files (`book1.txt`, `book2.txt`)

Processing:
1. Distributed ingestion of text files using SparkContext
2. Tokenization and normalization (lowercasing, punctuation removal)
3. Stop-word filtering
4. Mapping words into key-value pairs `(word, 1)`
5. Distributed aggregation using `reduceByKey`
6. Sorting by frequency

Output:
- Word counts saved as distributed output files

---

### Key Transformations

- `flatMap()` for tokenization
- `map()` to generate key-value pairs
- `reduceByKey()` for distributed aggregation
- `sortBy()` for ranking results

---

### Engineering Concepts Demonstrated

- Distributed batch ETL
- Functional transformations on RDDs
- Parallel aggregation
- Scalable file-based outputs
- Pipeline entrypoints and logging

---

### Use Case

This pipeline represents how large volumes of raw text data can be transformed into structured metrics for downstream analytics, search indexing, or reporting.
