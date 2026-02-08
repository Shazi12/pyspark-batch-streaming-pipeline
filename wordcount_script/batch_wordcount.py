from pyspark import SparkContext, SparkConf
import re
import logging

logging.basicConfig(level=logging.INFO)

# Common English stop words
STOP_WORDS = set([
    "i","me","my","myself","we","our","ours","ourselves","you","your","yours","yourself",
    "yourselves","he","him","his","himself","she","her","hers","herself","it","its","itself",
    "they","them","their","theirs","themselves","what","which","who","whom","this","that",
    "these","those","am","is","are","was","were","be","been","being","have","has","had",
    "having","do","does","did","doing","a","an","the","and","but","if","or","because","as",
    "until","while","of","at","by","for","with","about","against","between","into","through",
    "during","before","after","above","below","to","from","up","down","in","out","on","off",
    "over","under","again","further","then","once","here","there","when","where","why","how",
    "all","any","both","each","few","more","most","other","some","such","no","nor","not",
    "only","own","same","so","than","too","very","s","t","can","will","just","don","should",
    "now"
])

def process_text(line):
    words = re.findall(r'\b\w+\b', line.lower())
    return [word for word in words if word not in STOP_WORDS]

def run_batch_pipeline(input_paths, output_path):
    logging.info("Starting batch word count pipeline")

    conf = SparkConf().setAppName("BatchWordCount").setMaster("local[*]")
    sc = SparkContext.getOrCreate(conf)

    # Load and combine datasets
    rdd = sc.textFile(",".join(input_paths))

    # Pipeline transformations
    word_counts = (
        rdd
        .flatMap(process_text)
        .map(lambda word: (word, 1))
        .reduceByKey(lambda a, b: a + b)
        .sortBy(lambda x: -x[1])
    )

    # Save results
    word_counts.saveAsTextFile(output_path)

    logging.info("Pipeline completed successfully")
    sc.stop()

if __name__ == "__main__":
    run_batch_pipeline(
        input_paths=["datasets/book1.txt", "datasets/book2.txt"],
        output_path="output_wordcount"
    )
