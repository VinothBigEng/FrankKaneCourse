


object LearningByExampleSparkScala {


  import org.apache.spark.SparkContext
  import org.apache.spark._
  import org.apache.log4j._



  def main (args :Array[String]): Unit = {


    Logger.getLogger("org").setLevel(Level.ERROR)



    val sc = new SparkContext("local[*]",  "LearningByExampleSparkScala")
    val lines = sc.textFile( "C:\\FrankCaneCourse\\ml-100k\\ml-100k\\TestFile.txt")



    val pairsrdd = lines.map( x =>  (  x.split(" ")(0),1 ))

    val reducebykeyrdd = pairsrdd.reduceByKey( (x,y) =>  x + y )

    val groupby = pairsrdd



    val linesforwordcount = lines.flatMap( x => x.split("\\W+"))

    val words = linesforwordcount.map (x=> (x,1)).reduceByKey( (x,y) => x+ y)



    /*( x => (x.split("\\W+"), 1))*/


    words.foreach(println)
/*
     reducebykeyrdd.foreach(println)
    groupby.foreach(println)
*/

   /* pairsrdd.foreach(println) */


}}

