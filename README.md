## The 7 Ways to Code WordCount in Spark 2.0 
### Understanding the RDDs, Dataframes, Datasets & Spark SQL by Example

In this post, I would like to share a few code snippets that can help understand Spark 2.0 API. I am using the Spark Shell. But these code snippets can also be compiled on Scala IDE for Eclipse and executed on Hortonworks 2.5 as described in a previous article or Cloudera CDH sandboxes.
For illustration purposes, I am using the text file that contains the Humpty Dumpty rhyme. I am also printing the result on the console so that it becomes more fun to examine the output - rather than saving it into a file. In all examples I am reading the file, counting the words in each file while filtering out all other words except for Humpty and Dumpty.
Each of the snippets illustrates specific Spark construct(s). The examples highlight API functionaliy related to RDDs, Dataframes, Datasets or Spark SQL. 

## Example 1: Classic Word Count using filter & reduceByKey on RDD
     val dfsFilename = "/input/humpty.txt"
     val readFileRDD = spark.sparkContext.textFile(dfsFilename)
     val wcounts1 = readFileRDD.flatMap(line=>line.split(" "))
                               .filter(w => (w =="Humpty") || (w == "Dumpty"))
                               .map(word=>(word, 1))
                               .reduceByKey(_ + _)
     wcounts1.collect.foreach(println)

In this example each line in the file is read as an entire string into an RDD. Then each line is split into words. The split command generates an array of words for each line. The flatMap command flattens the array and groups them together to produce a long array that has all the words in the file. Then the array is filtered and only the two words are selected. Then each of the two words is mapped into a key/value pair. Last the reduceByKey operation is applied over the key/value pair to count the words’ occurrence in the text. 

## Example 2: Word Count Using groupBy on RDD
     val dfsFilename = "/input/humpty.txt"
     val readFileRDD = spark.sparkContext.textFile(dfsFilename)
     val wcounts2 = readFileRDD.flatMap(line=>line.split(" "))
                               .filter(w => (w =="Humpty") || (w == "Dumpty"))
                               .groupBy(_.toString)
                               .map(ws => (ws._1,ws._2.size))
     wcounts2.collect.foreach(println)
This example is similar to the first example. They only differ in the usage of groupBy command which generates a key/value pair that contains the word as a key and the sequence of the same word as a value. Then a new key/value pair is produced that uses the sequence size as a count of the occurrence of the word.  It is important to note that the filter predicate is applied on each words and only the words that satisfy the condition are passed to groupBy operation.


## Example 3: Word Count Using Dataframe. Row and groupBy
     val dfsFilename = "/input/humpty.txt"
     val readFileDF = spark.sparkContext.textFile(dfsFilename)
     val wordsDF = readFileDF.flatMap(_.split(" ")).toDF
     val wcounts3 = wordsDF.filter(r => (r(0) =="Humpty") || (r(0) == "Dumpty")).groupBy("Value").count()

     wcounts3.collect.foreach(println)

This example is totally different from the first two examples. Here we use Dataframes instead of RDD to work with the text as indicated with the “toDF” command. The returned Dataframe is made of a sequence of Rows. Because of the split operation, each row is made of one element only that can be accessed by the “0” index value. Also, simliar to 2nd example we are using the gourpBy operation followed by count to perform the word count.
The filter and groupBy operation above can also be written as follows:
     val wcounts3 = wordsDF.filter(r => (r.get(0) =="Humpty") || (r.get(0) == "Dumpty")).groupBy("Value").count()
Here the first element in the array within the row is accessed via “r.get(0)”.

## Example 4: Word Count Using Dataset 
     import spark.implicits._   

     val dfsFilename = "/input/humpty.txt"
     val readFileDS = spark.read.textFile(dfsFilename)
    
     val wcounts4 = readFileDS.flatMap(_.split(" "))
                              .filter(w => (w =="Humpty") || (w == "Dumpty"))
                              .groupBy("Value").count()
     wcounts4.collect.foreach(println)
Here we use Datasets instead of Dataframes to read the text file then we apply a filter and groupBy operation followed by count to perform the word count. This is the simplest and easiest to understand of all the examples in this article.

## Example 5: Word Count Using Spark SQL on Dataset & TempView
    import spark.implicits._  

    val readFileDS = spark.sqlContext.read.textFile(dfsFilename)
   
    val wordsDS = readFileDS.flatMap(_.split(" ")).as[String]
    wordsDS.createOrReplaceTempView("WORDS")    
    
    val wcounts5 = spark.sql("SELECT Value, COUNT(Value) FROM WORDS WHERE Value ='Humpty' OR Value ='Dumpty' GROUP BY Value")

    wcounts5.collect.foreach(println)

Here we create a Temporary View that we query using a Spark Select SQL statement.
  
## Example 6: Word Count Using Case Class, Dataset and where command

    case class CWord (Value: String)
    import spark.implicits._  
    
    val readFileDS = spark.sqlContext.read.textFile(dfsFilename).flatMap(_.split(" "))
    val CWordsDS = readFileDS.as[CWord]
    
    val wcounts6 = CWordsDS.where("Value = 'Humpty' OR Value = 'Dumpty'").groupBy("Value").count()
    wcounts6.collect.foreach(println)
   
In this example we utilize the power of Datasets by providing the schema as a case class. Then instead of using a filter on all the elements of the Dataset we use the “where” command and pass a predicate condition using the column name “Value”.

    ## Example 7: Word Count Using Case Class, Dataset, where and agg commands and $column-name
    case class CWord (Value: String)
    import spark.implicits._  
    
    val readFileDS = spark.sqlContext.read.textFile(dfsFilename).flatMap(_.split(" "))
    val CWordsDS = readFileDS.as[CWord]
    
    val wcounts7 = CWordsDS.where( ($"Value" === "Humpty") || ($"Value" === "Dumpty")).groupBy($"Value").agg(count($"Value"))
    
    wcounts7.collect.foreach(println)

In this example we utilize the power of Datasets by providing the schema as a case class. Then instead of a filter we use the “where” command and pass a predicate indicating using the column name via a $ variable. We also use the agg command which is the generalized command for all aggregate functions.

