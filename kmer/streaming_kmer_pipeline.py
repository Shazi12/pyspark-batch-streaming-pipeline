from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import os
import logging

logging.basicConfig(level=logging.INFO)

OUTPUT_DIR = "stream_output"


def generate_kmers(line, k=3):
    """Generate k-mer substrings from an input string."""
    return [line[i:i + k] for i in range(len(line) - k + 1)]


def save_kmer_counts(rdd):
    """
    Save RDD results to storage.
    NOTE: collect() used only for demo-scale streaming datasets.
    """
    if rdd.isEmpty():
        return

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    output_data = rdd.collect()

    with open(os.path.join(OUTPUT_DIR, "kmer_counts.txt"), "a") as file:
        for record in output_data:
            file.write(f"{record[0]}: {record[1]}\n")


def run_streaming_pipeline(host="localhost", port=9999):
    logging.info("Starting streaming k-mer pipeline")

    sc = SparkContext.getOrCreate()
    ssc = StreamingContext(sc, 10)

    # Stream ingestion
    lines = ssc.socketTextStream(host, port)

    # Transformations
    kmers = lines.flatMap(lambda line: generate_kmers(line))
    kmer_counts = kmers.map(lambda kmer: (kmer, 1)).reduceByKey(lambda a, b: a + b)

    # Output sink
    kmer_counts.foreachRDD(save_kmer_counts)

    # Print to console for monitoring
    kmer_counts.pprint()

    ssc.start()
    ssc.awaitTermination()


if __name__ == "__main__":
    run_streaming_pipeline()
