import org.apache.spark
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.log4j._
import org.apache.spark.rdd.RDD
import java.lang._
import org.apache.spark.SparkContext
import scala.io.Source._
import scala.io._
import java.io._

import org.apache.spark.rdd._


object SparkFixedWidthParser {

  def main(args: Array[String]){

    Logger.getLogger("org").setLevel(Level.ERROR)

      val spark = SparkSession
      .builder
      .appName("SparkSQL")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///C:/temp") // Necessary to work around a Windows bug in Spark 2.0.0;
      .getOrCreate()


    import spark.implicits._
    val ExperianRDD = spark.read.textFile("C:\\Users\\ABVP190\\Desktop\\Barclays ABSA\\BRAVO\\output_BLG2_20170721_ABSA_29345\\output_BLG2_20170721_ABSA_in_1_tworecords.txt").filter(!_.isEmpty)

    val ExperianExt = spark.sparkContext.textFile("C:\\Users\\ABVP190\\Desktop\\Barclays ABSA\\BRAVO\\output_BLG2_20170721_ABSA_29345\\ExperianSchema.csv").filter(!_.isEmpty)

    case class ExperianSchema(ExpColumnName:String,ExpDataType : String,ExpcolLength:Int)

    def mapper(line:String): ExperianSchema = {
      val fields = line.split(',')
      val ExpSchema:ExperianSchema = ExperianSchema(fields(0).toString, fields(1).toString,fields(2).toInt)
      return ExpSchema
    }

    val Extlengthseq = ExperianExt.map(mapper).map( x => x.ExpcolLength).collect().toSeq
//dont deete this  as converts into jasonschema.
    /*val WriteExperiantoJasonschema = ExperianExt.map(mapper).map(x => (x.ExpColumnName,x.ExpDataType,x.ExpcolLength)).toDF().select("*").write.json("C:\\Users\\ABVP190\\Desktop\\Barclays ABSA\\BRAVO\\output_BLG2_20170721_ABSA_29345\\ExperianSchema_Json.json")*/

    println(Extlengthseq)

    println (Extlengthseq.toDF().count())


    //val ExperianExttoSeq =ExperianExt.select("Length").collect().map(_(0)).toSeq.map(_.toString).map(_.toInt)

    //val ExperianExttoSeq =ExperianExt.select("*")
      //map(_(0)).toSeq.map(_.toString).map(_.toInt)

    //print(ExperianExttoSeq)

    def parseLinePerFixedLengths(LineData: String, lengthsofdata: Seq[Int]): Seq[String] =
    {
      //println("lengths",lengths)
     // println("indices",lengths.indices)
      lengthsofdata.indices.foldLeft((LineData, Array.empty[String]))
        {
          case ((linedatarem, fieldsofdata), idxoflength) =>
          //println("rem",linedatarem)

          val len = lengthsofdata(idxoflength)
            //println("len",len)
          val fld = linedatarem.take(len)
            //println("fld", fld)
            //val remdrop = linedatarem.drop(len)
            //println("remdrop",remdrop)
            (linedatarem.drop(len), fieldsofdata :+ fld)
        }._2
    }

     val fieldsofData = ExperianRDD.
        map(parseLinePerFixedLengths(_, Extlengthseq)).withColumnRenamed("value", "fields")


    val ExperianDF = Extlengthseq.indices.foldLeft(fieldsofData)
    { case (result, idx) =>
      result.withColumn(s"col_$idx", $"fields".getItem(idx))
    }

    val ExperianDFtoFile = ExperianDF.drop("fields")
    println("removing fields value and adding the default column headers")
    ExperianDFtoFile.show()
    println("writing the output with pipe delimited csv")
    ExperianDFtoFile.write.mode("overwrite").format("com.databricks.spark.csv").option("delimiter", "|").save("C:\\Users\\ABVP190\\Desktop\\Barclays ABSA\\BRAVO\\output_BLG2_20170721_ABSA_29345\\output_tworecords")

  } }

