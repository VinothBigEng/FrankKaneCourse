import scala.io.Source
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql._
import org.apache.log4j._



object IngestExperianwithJson {

  def main (args : Array [String]) ={

  Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .appName("IngestExperianwithJson")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///C:/temp")
      .getOrCreate()

      import spark.implicits._

      val ExperianReadSchemaRDD = spark.sparkContext.textFile("C:\\Users\\ABVP190\\Desktop\\Barclays ABSA\\BRAVO\\output_BLG2_20170721_ABSA_29345\\ExperianSchema_13107_Businesnames.csv").filter(!_.isEmpty)
      val ExperianDataRDD  = spark.sparkContext.textFile("C:\\Users\\ABVP190\\Desktop\\Barclays ABSA\\BRAVO\\output_BLG2_20170721_ABSA_29345\\output_BLG2_20170721_ABSA_in_1.txt").filter(!_.isEmpty)

      val Extlengthseq:Seq[Int]= spark.read.json("C:\\Users\\ABVP190\\Desktop\\Barclays ABSA\\BRAVO\\Experianjsonschema2.json").select("Metadata.width")
                                .collect()
                                .map(_(0))
                                .toSeq.map(_.toString).map(_.toInt)


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

    val ExperiandatatoArray = ExperianDataRDD.map(parseLinePerFixedLengthsSubstr (_, Extlengthseq ))


    ExperiandatatoArray.map(experiandatastr(_)).toDF().write.mode("overwrite").option("delimiter", "|").csv("C:\\Users\\ABVP190\\Desktop\\Barclays ABSA\\BRAVO\\output_BLG2_20170721_ABSA_29345\\ExperianData13107")

  }

}
