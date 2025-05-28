# PySpark RDD Operations Assignment

This repository contains a collection of PySpark tasks and assignments for the ITI 'Spark & PySpark for Big Data' course.

## Assignment Overview

The notebook [RDD Assignment/H.W_NP_RDD_Operations_Trainees.ipynb](RDD%20Assignment/H.W_NP_RDD_Operations_Trainees.ipynb) demonstrates the following PySpark RDD operations:

- Creating a SparkContext using `SparkSession`
- Creating and displaying RDDs from Python lists
- Reading text files into RDDs and displaying elements
- Counting rows in an RDD
- Transforming RDD data (lowercasing, splitting)
- Removing stopwords from text data using `flatMap`
- Filtering words starting with a specific character
- Reducing data by key and summing values with `reduceByKey`
- Creating key-value pair RDDs and performing join operations

## How to Run

1. Ensure you have PySpark installed and a compatible Python environment.
2. Open the notebook [RDD Assignment/H.W_NP_RDD_Operations_Trainees.ipynb](RDD%20Assignment/H.W_NP_RDD_Operations_Trainees.ipynb) in Jupyter Notebook or VS Code.
3. Run the cells sequentially to execute the RDD operations.

## Example Tasks

- **Create RDD from list:**  
  ```python
  rdd1 = sc.parallelize([('JK', 22), ('V', 24), ...])
  ```
- **Read text file into RDD:**  
  ```python
  sample1 = sc.textFile("sample1.txt")
  ```
- **Remove stopwords:**  
  ```python
  stopwords = [...]
  def remove_stopwords(line):
      return [word for word in line if word not in stopwords]
  sample1_no_stopwords = sample1_lower.flatMap(remove_stopwords)
  ```
- **Join RDDs:**  
  ```python
  rdd1.join(rdd2).collect()
  ```

See the notebook for full code and outputs.

---
> **Note:** This repository will be continuously updated with new assignments and materials as the course progresses. Stay tuned for the latest content!