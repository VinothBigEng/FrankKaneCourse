import org.apache.spark.SparkContext
import org.apache.spark._
import org.apache.log4j._
import org.apache.spark.sql._


import sys.process._

object Createhadoopfolder {

  def main (args : Array[String]) ={

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .appName("Test")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///C:/temp")
      .getOrCreate()

   val test = "dir".!


  }

}
