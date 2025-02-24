# Test Topics
> the test will cover all topics from the start of the course *up to and including **lab 5***

## Chapter 14 Big Data and NoSQL

- **Big Data**: a dataset which meets the `3 V's` requirements that make it unsuitable for normal data processing
- **Need for big data**: Companies growing following the burst of the dot com bubble saw a massive increase to the volume of data that required processing. as a resilt, they developed solutions to storing and managing larger data sets

### The 3 V's of Big data

1. **Volume**: The quantity of data to be stored
2. **Velocity**: the rate at which data enters the system
3. **Variety**: Variations on structure of data to be stored (example - variations in recieved file formats)

#### Other V's of big data

4. Variability: Data's meaning  depending on the context
5. Veracity: wether the incoming data is accurate
6. Value: wether the incoming data provides any meaningful infomration
7. Visualization: wether the data can be representing in any way to make it understandable

## Hadoop HDFS

### Basic Characteristics

- High Volume: Hadoop assumes files sizes will be very large (upwards to petabytes of data)
- *Write once, read many philosophy*: a file is created, written to, then closed and not to be modified after. 
- Stream /access: Hadoop is optimized for batch processing of an entre file as a continuous stream of data
- Fault Tolerance: Hadoop is designed to replicate data across many devices, so if one device fails the data is still availbale. Hadoop is typically run using a distributed network of systems. sp individual device failure is an expected part off processing. default hadoop replication factor is 3 devices.

### Different Node Types
> name node, data node...
- Data Node: contains the actual data stored of the hdfs; broken into blocks and replicated acros devices
- name node: Contains metadata for the system; one name node per hdfs; contains name of each file, block numbers that comprise each file, and desired replication for each fiole
- client node: makes requests to the file system to read files or write new files

### basic commands

- cat: copies data of file to standard output
- put: upload a file from local to hdfs
- get: copy file from hdfs to local

### deployment nodes

- standalone: uses one node or machine
psuedo distributeD: one node that simulated many nodes; dauemons are run on seperate processes in the jvm
- full distributed: daemons ares splut across a variery of nodes/machines (such as google cloud)

## Hadoop MapReduce
> this will most likely appear on the test as a short answer question

- **basic characteristics**: Mapper & Reducer
- **Map function**: Takes a collection of data, sorts and filters it into key-value pairs
- **Reduce function**: Takes a set of key-value pairs and summarizes them into a single result

Steps for creating and running a map reduce program:
1. Create a java project with Hadoop and Hadoop map-reduce packages
2. Create a mapper class
3. Create a reducer class
4. Create a main file to run the other classes
5. Generate a jar file
6. Upload the jar file to Hadoop
7. Run the jar file with hdfs dfs file-name-here

- Drawbacks of using MapReduce ( why Spark is more popular )
    - Worse performance compared to spark
    - have to write code in java which isn't popular in Big Data fields
- Be able to explain the process of how a map reduce program can solve a given problem, with specific details given for the mapper and reducer. -

**Ex. Create a word counter for a text file**
**Answer**: 
- Create a mapper class which will read through the contents of a text file line by line. 
- For each line, split based on spaces to get each word of the line. 
- Then for each word, add to a collection a key value pair where the key is the word and the value is integer literal 1. 
- Then create a reducer class, which will take a collection produced in the mapper, loop through the values, and 
- add to a new collection the original key and the count of the values associated with that key.

## Spark
- Advantages Spark has vs MapReduce
- Spark is much faster in performance
- MapReduce designed for batch processing, Spark for real-time processing
- Spark supports many languages (Python, SQL, Scala, Java, R) ,
MapReduce only supports java
- Spark has better fault tolerance
- Spark has more extensive ecosystem and integrates easily with other tools,
MapReduce mainly works with HDFS
-Managed Spark Deployments
- Google Dataproc - Allows serverless running of Spark jobs with
auto-scaling resources
- Databricks - Allows for running of spark through deployment to virtual
machines
-Understand the process of batch processing - Processing of tasks or
transactions in batches, typically runs automatically on a set period of time
-Data Sources used by Spark ( ex// csv ,json, text ) -
- Text - process text line by line, can include headers in first line of file
- Csv - values are comma separated, schema can be inferred by spark
- json - values split into objects, attributes can be referenced in code, schema
can be inferred by spark
-Spark RDD vs Dataframe - RDD was original, Dataframe preferred because they
have schemas and works with high level API
Spark Dataframe
-How schema is added to a dataframe - Way of identifying each of the columns
and knowing their data types. DF is like a table in a DB
-lazy vs eager evaluation - Lazy = When doing transformations (selecting,filtering
columns) spark does not execute anything until it needs to output something, once
out is asked for processing begins
-How data frame transformations work - transformations perform some
modification to a dataset and then produces a new dataset
-Examples of various transformations -
- Select - same as SQL
- filter - same as SQL where clause
- distinct - returns only unique elements in
- sort - sorts the data set with option for ascending or descending
- sample - takes a subset of a data set, with option for replacement
- groupBy - same as SQL
- join - same as SQL
- withColumn - returns a new dataframe with the column name specified
modified in some way
- drop - removes a column from a dataframe
-Examples of various actions -
- show - prints the first x number of rows of a dataframe
- first - prints the first row of a dataframe
- take - returns the first x rows as a list
- collect - returns all records of a dataframe as a list
- count - returns the number of rows in a dataframe or column
Spark SQL
-Steps for using sql with a DataFrame - Take a DF, create a view (table in
memory), query view with spark database, allows users to execute SQL statements
directl




> Document Content Credidted to [*Julia Forward*](https://github.com/UnusualFrog)

