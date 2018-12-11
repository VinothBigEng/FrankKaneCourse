import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql
import org.apache.spark._
import org.apache.spark.sql.SparkSession


object  Enceladus_Testing {


  def main (args :Array[String]) = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .appName("Enceladus_testing")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///C:/temp")
      .getOrCreate()



    val rawdf = spark.read.format("com.databricks.spark.xml").option("inferschema","true").load("C:\Users\ABVP190\Desktop\Test_Trade_scalr.txt")


    rawdf.printSchema()



  }




}
