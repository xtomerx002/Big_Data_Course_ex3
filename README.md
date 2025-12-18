# Big_Data_Course_ex3

---

## Exercise 1

### Q1. Code

```python
import sys
from pyspark import SparkContext, SparkConf

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: wordcount <input_folder>", file=sys.stderr)
        sys.exit(-1)

    conf = SparkConf().setAppName("python-word-count")
    sc = SparkContext(conf=conf)

    text_file = sc.textFile("hdfs://" + sys.argv[1])

    counts = (
        text_file
        .flatMap(lambda line: line.split(" "))
        .map(lambda word: (word, 1))
        .reduceByKey(lambda a, b: a + b)
        .filter(lambda x: len(x[0]) > 5)
        .repartition(5)  # ensure 5 tasks
    )

    # takeOrdered is an action
    result = counts.takeOrdered(40, key=lambda x: -x[1])

    print("--------------------------------------------")
    print(repr(result)[1:-1])
    print(*result, sep="\n")
    print("--------------------------------------------")
```

### Q2. Add print-screen of the stage proving you have 5 tasks

<img width="975" height="72" alt="image" src="https://github.com/user-attachments/assets/c5c40a85-7f10-4e3b-b168-a0952d73fe91" />


---

## Exercise 2
### Q1. Code

```python
import sys
from pyspark import SparkContext, SparkConf

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: wordcount <input_folder>", file=sys.stderr)
        sys.exit(-1)

    conf = SparkConf().setAppName("python-word-count")
    sc = SparkContext(conf=conf)

    text_file = sc.textFile("hdfs://" + sys.argv[1])

    words_rdd = text_file.flatMap(lambda line: line.split(" ")).cache()

    counts = (
        words_rdd
        .map(lambda word: (word, 1))
        .reduceByKey(lambda a, b: a + b)
        .filter(lambda x: len(x[0]) > 5)
        .repartition(5)
    )

    distinct_count = words_rdd.distinct().count()

    result = counts.takeOrdered(40, key=lambda x: -x[1])

    print("--------------------------------------------")
    print(repr(result)[1:-1])
    print(*result, sep="\n")
    print("--------------------------------------------")
    print("Total distinct words:", distinct_count)
```

### Q2. Write the number of words found

<img width="708" height="70" alt="image" src="https://github.com/user-attachments/assets/62fec00b-bbb4-4e7a-8b35-7349f8881c7d" />

---

## Exercise 3
Put a print-scrin with the DAG of the first stage, which shows it reads the files from s3a://<your_bucket_name> 

<img width="561" height="919" alt="image" src="https://github.com/user-attachments/assets/0b4114ce-5eba-4498-bbba-c2b33932f44c" />

---

## Exercise 4

### Q1. Code

```python
import sys
from pyspark import SparkContext, SparkConf

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: wordcount <input_folder>", file=sys.stderr)
        sys.exit(-1)

    conf = SparkConf().setAppName("python-word-count")
    sc = SparkContext(conf=conf)

    text_file = sc.textFile("s3a://" + sys.argv[1])

    words_rdd = text_file.flatMap(lambda line: line.split(" ")).cache()

    counts = (
        words_rdd
        .map(lambda word: (word, 1))
        .reduceByKey(lambda a, b: a + b)
        .filter(lambda x: len(x[0]) > 5)
    )

    distinct_count = words_rdd.distinct().count()

    result = counts.takeOrdered(40, key=lambda x: -x[1])

    print("--------------------------------------------")
    print(repr(result)[1:-1])
    print(*result, sep="\n")
    print("--------------------------------------------")
    print("Total distinct words:", distinct_count)

    filtered = words_rdd.filter(lambda w: w.isalpha() or w.rstrip('.,').isalpha())

    sample = filtered.take(1)
    if sample:
        longest = filtered.reduce(
            lambda a, b: a if len(a.rstrip('.,')) >= len(b.rstrip('.,')) else b
        )
        print(f"Longest word: '{longest}' (length: {len(longest.rstrip('.,'))})")
```

### Q2. Put here the printout of the longest word:

<img width="766" height="56" alt="image" src="https://github.com/user-attachments/assets/05fd1ff4-c7c3-4cd4-89b1-966bc68a5b4f" />


---

## Exercise 5

### Q1. Code

```python
import sys
from pyspark import SparkContext, SparkConf

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: count_words_in_line <input_bucket_or_path>", file=sys.stderr)
        sys.exit(-1)

    conf = SparkConf().setAppName("count-words-in-line")
    sc = SparkContext(conf=conf)

    text_file = sc.textFile("s3a://" + sys.argv[1])

    line_counts = text_file.map(lambda line: (line, len(line.split())))

    max_line, max_words = line_counts.reduce(
        lambda a, b: a if a[1] >= b[1] else b
    )

    print("Line with the most words:")
    print(max_line)
    print("Number of words in this line:", max_words)
```

### Q2. Put here the printout of the line with the most words:

<img width="975" height="203" alt="image" src="https://github.com/user-attachments/assets/3306f74f-0c70-420e-b404-abb4530b03d0" />


---
