import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructField

object LoadFixedWidthInSpark {

  import org.apache.spark.SparkContext
  import org.apache.spark.SparkContext._
  import org.apache.spark._
  import org.apache.log4j._
  import org.apache.spark.sql.SQLContext

  /*import scala.io.Source
  import org.apache.spark.rdd.RDD
  import org.apache.spark.sql.types.DataType
  import org.apache.parquet.schema.MessageType
  import org.apache.spark.sql.types.StructType
  import scala.collection.JavaConverters._
  import org.apache.parquet.schema.GroupType
  import org.apache.spark.sql.types.IntegerType
  import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName._
  import org.apache.parquet.schema.OriginalType._
  import org.apache.parquet.schema.Type.Repetition._
  import org.apache.spark.sql.types.BooleanType
  import org.apache.spark.sql.types.LongType
  import org.apache.spark.sql.types.FloatType
  import org.apache.spark.sql.types.DoubleType
  import org.apache.spark.sql.types.BinaryType
  import org.apache.spark.sql.types.StringType
  import org.apache.spark.sql.types.ArrayType
  import org.apache.spark.sql.types.DateType
  import org.apache.spark.sql.types.DecimalType
  import org.apache.spark.sql.types.DataType */
  import org.apache.spark.SparkContext

  import org.apache.spark._
  import org.apache.spark.SparkContext._
  import org.apache.spark.sql._
  import org.apache.log4j._
  import scala.io.Source


  def main (args :Array [String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "LoadFixedWidthInSpark")

    val sqlcontext = new SQLContext(sc)

    val spark = SparkSession
      .builder
      .appName("SparkSQL")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///C:/temp") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
      .getOrCreate()


    val pathfortextfile =  sc.textFile ("C:\\Users\\ABVP190\\Desktop\\Barclays ABSA\\BRAVO\\output_BLG2_20170721_ABSA_29345\\output_BLG2_20170721_ABSA_in_1_onerecord.txt")

    val datafile = spark.sparkContext.textFile("C:\\Users\\ABVP190\\Desktop\\Barclays ABSA\\BRAVO\\output_BLG2_20170721_ABSA_29345\\output_BLG2_20170721_ABSA_in_1_onerecord.txt")

    val datafiledataframe = spark.read.json("C:\\Users\\ABVP190\\Desktop\\Barclays ABSA\\BRAVO\\output_BLG2_20170721_ABSA_29345\\output_BLG2_20170721_ABSA_in_1.txt")

    val jsonschema = sqlcontext.read.json("C:\\Users\\ABVP190\\Desktop\\Barclays ABSA\\Forex_POC\\Forex_Metadata_7columns_Experian.json")


    val first10 = datafile.take(10)


    val first10_2 = pathfortextfile.take(10).toString

    val first01 = datafiledataframe.take(10)

 /*   first10.foreach(println) */

    case class  ExperianDataframe (Bureau :Int , Somevlaue : Int)

    val columns = first10.map(data =>  (data.substring(0,3), data.substring (98731,98738)))

      /*.map ( {
      case (e1,e2) => ExperianDataframe (e1.toInt , e2.toInt)
    })
 */

   /* for  (data <- first10 ) {


      val fields  = data.split("")

      fields.foreach(println)

    } */




 /* val getfile = Source.fromFile("C:\\Users\\ABVP190\\Desktop\\Barclays ABSA\\BRAVO\\output_BLG2_20170721_ABSA_29345\\output_BLG2_20170721_ABSA_in_1_onerecord.txt").getLines()


      val outputdata = new Array[String] (getfile.length)

     println(getfile)
*/

       /*columns.foreach(println) */

  val arrayofdata = pathfortextfile.map( x=> Array(x)).toString()


    arrayofdata.foreach(println)

/*
  val lenghtfirst10 :Seq[Int]= Seq (1,2,3,4,5,6,7,8,9)
          val dataforparaseing= first10.toIterable /*.map (x => x)*/




     var start = 0

      def getStringFromSeq(val_lengths :Seq[Int],line_data: String = dataforparaseing.mkString, delimiter_length:Int = 0): Seq[String] = {

        //for loop - order needs to be maintained
        val seqAccum = new Array[String](val_lengths.length)

        for (i <- 0 to val_lengths.length -1) {
          seqAccum(i) = line_data.substring(start, start + val_lengths(i)).trim()
          start = start + val_lengths(i) + delimiter_length
        }
        seqAccum.toSeq
      }

      val prasedata = getStringFromSeq(lenghtfirst10,dataforparaseing.toString,0 )

      prasedata.foreach(println)*/

  }
}



/*first10.foreach(println) */

    /*first01.foreach(println)*/

  /*   case class Experian(Brueau:Int, name:String)


    def mapper (Line:String) :Unit ={
      val Column1 = Line.substring(1,3)
      val column2 = Line.substring(4,9)

      return (Column1,column2)


    }

    val people = first10.map(mapper).toString

    people.foreach(println)


   /* def loadExternalJsonSchema(path: String) = {
      DataType.fromJson(Source.fromFile(path).getLines().mkString("\n")).asInstanceOf[StructType]

         } */

    /*def loadJsonSchemanotStrutype  (PathofJson:String): Unit = {

    } */

    /*val expschema = loadExternalJsonSchema("C:\\Users\\ABVP190\\Desktop\\Barclays ABSA\\Forex_POC\\Experian_Json.json")

    expschema.printTreeString() */


  /*  val pathforjson  = "C:\\Users\\ABVP190\\Desktop\\Barclays ABSA\\Forex_POC\\Experian_Json.json"


    val outputdir = "C:\\Users\\ABVP190\\Desktop\\Barclays ABSA\\Forex_POC\\testFile.txt"

    val jsonstring = sc.wholeTextFiles(pathforjson).first._2




 print(jsonstring) */


    /** val fixedwidthpraser = new FixedWidthParser(first10.toString) */




    /* def readExprianData (filepath :String , Jasonpath : String): Unit = {
      read
    } */







  var start = 0;
  val input = first10.toString;
  val line_len = first10.length

  def getStringFromSeq(val_lengths :Seq[Int],line_data: String = input, delimiter_length:Int = 0): Seq[String] = {

    //for loop - order needs to be maintained
    val seqAccum = new Array[String](val_lengths.length)

    for (i <- 0 to val_lengths.length -1) {
      seqAccum(i) = line_data.substring(start, start + val_lengths(i)).trim();
      start = start + val_lengths(i) + delimiter_length;
    }
    seqAccum.toSeq
  }
  }

 } */




