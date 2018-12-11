import scala.io.Source
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql._
import org.apache.log4j._

object ReadStructtypetoSeqInt {

  def main (args : Array [String]) = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .appName("ReadStructtypetoSeqInt")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///C:/temp")
      .getOrCreate()

    import spark.implicits._
    //val Extlengthseq = spark.read.json("C:\\Users\\ABVP190\\Desktop\\Barclays ABSA\\BRAVO\\Experian_test_schema.json").show()
  //val testdata =   DataType.fromJson(Source.fromFile("C:\\Users\\ABVP190\\Desktop\\Barclays ABSA\\BRAVO\\Experian_test_schema.json").getLines().mkString("\n")).asInstanceOf[StructType]

    def loadJsonSchema(path: String) = {
      DataType.fromJson(Source.fromInputStream(this.getClass().getResourceAsStream(path)).getLines().mkString("\n")).asInstanceOf[StructType]
    }

    def loadExternalJsonSchema(path: String) = {
      DataType.fromJson(Source.fromFile(path).getLines().mkString("\n")).asInstanceOf[StructType]
    }


    //val strucytread =loadJsonSchema("C:\\Users\\ABVP190\\IdeaProjects\\FrankCaneCourse\\src\\main\\experianschema.json")


    //val structread2 = loadExternalJsonSchema("C:\\Users\\ABVP190\\Desktop\\Barclays ABSA\\BRAVO\\Experian_test_schema.json")

    val structread2 = loadExternalJsonSchema("C:\\Users\\ABVP190\\Desktop\\Barclays ABSA\\BRAVO\\output_BLG2_20170721_ABSA_29345\\ExperianSchema_13037_struct.json")

//strucytread.foreach(println)
 //structread2.foreach(println)


    val length_seq:Seq[Int] = structread2.map(_.metadata.getLong("width").toInt)

println(length_seq)



  }


}
