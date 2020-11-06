import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD._

/**
  * Created by olivera on 11/6/20.
  */
object test_hello_world {

  def main(args: Array[String]): Unit = {
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder()
      .master("local[24]")
      .appName("Spark Hello world example")
      .getOrCreate()
    import spark.implicits._


    val textFile = spark.read.textFile("README.md")

    /* this will print lines from the file **/
    //textFile.collect().foreach(println)

    /*this will count word pairs in the text file **/
    /*val count = textFile.flatMap(line => line.split(" ")) // split each line based on spaces
      .map(word => (word, 1)) //map each work into a (word, 1) pair
      .rdd // convert DataFrame into rdd
      .reduceByKey(_+_) //reduce
      .foreach(println) // print each word and its counter  **/

    /*this will count the number of words in the file **/
    //val wordnum = textFile.flatMap(line => line.split(" ")).count()
    //println(wordnum)

    /*this will sort the words by its occurrences in the file in descending order **/
    val rankdf = textFile.flatMap(line => line.split(" ")) // split each line based on spaces
        .map(word => (word, 1)) //map each work into a (word, 1) pair
        .rdd // convert DataFrame into rdd
        .reduceByKey(_+_) //reduce
      .toDF() //return to DataFrame to perform ranking

    rankdf.orderBy($"_2".desc).show()
    
}

}
