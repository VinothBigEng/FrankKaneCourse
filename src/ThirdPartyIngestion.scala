import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql._
import org.apache.log4j._
import org.apache.spark.SparkContext

import scala.io.Source



object ThirdPartyIngestion {


  def main (args :Array [String]) ={

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .appName("ThirdPartyIngestion")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///C:/temp")
      .getOrCreate()

    import spark.implicits._

    val ExperianDataRDD  = spark.sparkContext.textFile("C:\\Users\\ABVP190\\Desktop\\Barclays ABSA\\BRAVO\\output_BLG2_20170721_ABSA_29345\\output_BLG2_20170721_ABSA_in_1.txt").filter(!_.isEmpty)

    def loadExternalJsonSchema(path: String) = {
      DataType.fromJson(Source.fromFile(path).getLines().mkString("\n")).asInstanceOf[StructType]
    }

    def loadHDFSJsonSchema(path: String, sc: SparkContext) = {
      DataType.fromJson(sc.textFile(path).collect().mkString("\n")).asInstanceOf[StructType]
    }

    val ExperianStructread = loadExternalJsonSchema("C:\\Users\\ABVP190\\Desktop\\Barclays ABSA\\BRAVO\\output_BLG2_20170721_ABSA_29345\\ExperianSchema_13037_struct.json")

    val length_seq:Seq[Int] = ExperianStructread.map(_.metadata.getLong("width").toInt)

    println (length_seq)


    def experiandatastr (linedata :Seq[String]) :String = {
      linedata.mkString("|")
    }


    def parseLinePerFixedLengthsSubstr(LineData: String, lengthsofdata: Seq[Int]): Seq[String] ={
      var start = 0

      {
        //for loop - order needs to be maintained
        val seqAccum = new Array[String](lengthsofdata.length)
        //val input = LineData;
        val line_len = LineData.length
        for (i <- 0 to lengthsofdata.length -1) {
          seqAccum(i) = LineData.substring(start, start + lengthsofdata(i)).trim()
          start = start + lengthsofdata(i)
        }
        seqAccum.toSeq
      }
    }


    val ExperiandatatoArray = ExperianDataRDD.map(parseLinePerFixedLengthsSubstr (_, length_seq ))


    ExperiandatatoArray.map(experiandatastr(_)).toDF().write.mode("overwrite").option("delimiter", "|").csv("C:\\Users\\ABVP190\\Desktop\\Barclays ABSA\\BRAVO\\output_BLG2_20170721_ABSA_29345\\ExperianData13037")



  }
}
