import org.apache.calcite.avatica.ColumnMetaData.StructType
import org.apache.spark
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.log4j._
import org.apache.spark.rdd.RDD

import scala.io.Source._
import scala.io._

import java.io._




object TestFixedWidthParser {

  def main (args :Array [String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark :SparkSession = SparkSession
      .builder
      .appName("TestFixedWidthParser")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///C:/temp") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
      .getOrCreate()

    /*   val sc = new SparkContext("local[*]", "SparkFixedWidthParser")*/


    val textfilereader =  spark.read.option("header","false").text("C:\\Users\\ABVP190\\Desktop\\Barclays ABSA\\BRAVO\\output_BLG2_20170721_ABSA_29345\\output_BLG2_20170721_ABSA_in_1_tworecords.txt")

//    val textfileasArray =  spark.sparkContext.textFile("C:\\Users\\ABVP190\\Desktop\\Barclays ABSA\\BRAVO\\output_BLG2_20170721_ABSA_29345\\output_BLG2_20170721_ABSA_in_1_tworecords.txt")

    def getStringFromSeq(val_lengths :Seq[Int],line_data: String ) :String = {
      var start = 0
      val seqAccum = new Array[String](val_lengths.length)


      for (i <- 0 to val_lengths.length -1) {
        seqAccum(i) = line_data.substring(start, start + val_lengths(i)).trim
        start = start + val_lengths(i)
      }
      seqAccum.mkString("|")

    }



    val lenghttoextract :Seq[Int]= Seq (3,6,10,12,10,10,12,1,1,1,3,1,1,1,15,8,1,1,8,15,3,6,8,50,1,2,10,8,2,15,15,15,25,8,25,25,25,10,4,8,25,25,25,10,4,8,25,25,25,10,4,10,10,10,20,8,10,10,10,20,8,16,16,16,16,8,8,72,8,72,8,72)



    /*val rows = textfilereader.select("*").collect().map(_.getString(0)).mkString("\n") */

    /* val rows = textfilereader.select("*").collect().map(_.getString(0)).mkString("\n")

    println(rows)




    val lenghttoextract :Seq[Int]= Seq (3,6,10,12,10,10,12,1,1,1,3,1,1,1,15,8,1,1,8,15,3,6,8,50,1,2,10,8,2,15,15,15,25,8,25,25,25,10,4,8,25,25,25,10,4,8,25,25,25,10,4,10,10,10,20,8,10,10,10,20,8,16,16,16,16,8,8,72,8,72,8,72)

    val WriteFile = new  PrintWriter(new File( "C:\\Users\\ABVP190\\Desktop\\Barclays ABSA\\BRAVO\\output_BLG2_20170721_ABSA_29345\\Test.txt"))


    def getStringFromSeq(val_lengths :Seq[Int],line_data: String ) :String = {
      var start = 0
      val seqAccum = new Array[String](val_lengths.length)


      for (i <- 0 to val_lengths.length -1) {
        seqAccum(i) = line_data.substring(start, start + val_lengths(i)).trim
        start = start + val_lengths(i)
      }
      seqAccum.mkString("|")

    }


    val prasedata = getStringFromSeq(lenghttoextract, rows)

    println(prasedata)

    /*
    while (CountofRows !=0 ) {
      val prasedata = getStringFromSeq(lenghttoextract, rows)
      println("output:" + prasedata)

    }*/



    /*
       for ( i <- 1 to  rows.length-1 ){
          val prasedata = getStringFromSeq(lenghttoextract, rows(i))
         WriteFile.write(prasedata)
          WriteFile.close()

          //val parallelizeRDD = spark.sparkContext.parallelize(prasedata).saveAsTextFile("C:\\Users\\ABVP190\\Desktop\\Barclays ABSA\\BRAVO\\output_BLG2_20170721_ABSA_29345\\Testfile")
        }
  */

    /* val paralleize = spark.sparkContext.parallelize(prasedata).
       saveAsObjectFile("C:\\Users\\ABVP190\\Desktop\\Barclays ABSA\\BRAVO\\output_BLG2_20170721_ABSA_29345\\Test2.txt")


         println("outPut:"+prasedata)

     WriteFile.write(prasedata)

     WriteFile.close() */

    /*     val df = spark.createDataset(prasedata)


    df.printSchema() */

*/
  } }

