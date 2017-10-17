# Analyzing a Large Graph with Spark/Scala

Your task is to calculate the gross accumulated node weights for each node in graph1.tsv and graph2.tsv from edge weights using Spark and Scala. Assume the graph to be a representation of a network flow where each edge represents the number of items flowing from source to target. The
gross accumulated node weight for a node is now defined as the number of items
produced/consumed by the node, and can be evaluated using the following formula:
>*Σ(all incoming edge weights) − Σ(all outgoing edge weights)*

You should perform this task using the [DataFrame API](https://spark.apache.org/docs/1.6.1/api/scala/index.html#org.apache.spark.sql.DataFrame) in Spark. [Here](https://spark.apache.org/docs/1.6.1/sql-programming-guide.html) is a guide that will help you get started on working with data frames in Spark.
Load your input data into a data frame by inferring the schema using reflection (Refer to the guide
above). Filter out (ignore) all edges whose edge weights equal 1 i.e., only consider edges whose edge weights do not equal 1. Then use other DataFrame operations to calculate the gross accumulated node weight for each node.
You may find some of the following DataFrame operations helpful:
>toDF, filter, join, select, groupBy  

Consider the following example:
**Input:**  
|src |tgt |weight  |  
| ------------- |:-------------:| -----:|  
|1|2|40|  
|2|3|100|  
|1|3|60|  
|3| 4|1|  
|3| 1|10|  

**Output:**
|src |tgt |weight  |
| ------------- |:-------------:| -----:|
|1| -90| = (10) - (40 + 60)|
|2| -60| = (40) - (100)|
|3| 150| = (100 + 60) - (10)|
Notice here that the edge from 3 to 4 is ignored since its weight is 1.
Your Scala program should handle the same two arguments as in Question 1 for input and output
from the console, and should generate the same formatted output file on the supplied output directory (tab-separated-file). Please note that the default Spark saveastextfile method uses a saving format that is different from Hadoop’s, so you need to format the result before saving to file (Tip: use map and mkString). The result doesn’t need to be sorted.

# Prerequisites
#### Installing Virtual Machine (VM)

  - Virtual Machine platform ([VirtualBox](www.virtualbox.com), [Cloudera](https://www.cloudera.com/downloads/quickstart_vms/5-8.html))
  - Configure the VM based on [this instruction](http://poloclub.gatech.edu/cse6242/2017spring/hw3/VMSetup.pdf)
#### Setting up Development Environments
We found that compiling and running Hadoop/Scala code can be quite complicated. So, we have prepared some skeleton code, compilation scripts, and execution scripts for you that you can use, in the HW3 skeleton folder. You should use this structure to submit your homework. In the GitHub directories, you will find **pom.xml** , **run1.sh**, **run2.sh** and the **src** directory.
* The src directory contains a main Java/Scala file that you will primarily work on. We have provided some code to help you get started. Feel free to edit it and add your files in the directory, but the main class should be Q1 and Q2 accordingly. Your code will be evaluated using the provided run1.sh and run2.sh file (details below).
* pom.xml contains the necessary dependencies and compile configurations for each
question. To compile, you can simply call Maven in the corresponding directory by this command:
```sh
mvn package
```
It will generate a single JAR file in the target directory (i.e., target/q2-1.0.jar). Again, we have provided you some necessary configurations to simplify your work for this homework, but you can edit them as long as our run script works and the code can be compiled using mvn package command.
*run1.sh, run2.sh are the script files that run your code over graph1.tsv (run1.sh) or graph2.tsv (run2.sh) and download the output to a local directory. The output files are named based on its question number and graph number (e.g. q1output1.tsv). You can use these run scripts to test your code. Note that these scripts will be used in grading.

Here’s what the above scripts do:
1. Run your JAR on Hadoop/Scala specifying the input file on HDFS (the first
argument) and output directory on HDFS (the second argument)
2. Merge outputs from output directory and download to local file system.
3. Remove the output directory on HDFS.
