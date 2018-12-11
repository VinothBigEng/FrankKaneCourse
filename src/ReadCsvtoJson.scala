import java.io.{File, PrintWriter}

import scala.io.Source
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql._
import org.apache.log4j._



object ReadCsvtoJson {

  def main (args : Array[String])= {

    Logger.getLogger("org").setLevel(Level.ERROR)


    val inputfile = "C:\\Users\\ABVP190\\Desktop\\Barclays ABSA\\BRAVO\\output_BLG2_20170721_ABSA_29345\\ExperianSchema_13107_struct.csv"
    val readcsvtojson = Source.fromFile(inputfile).getLines().toArray
    readcsvtojson.foreach(println)


    val outputData = new Array[String] (readcsvtojson.length)
    var finalOutput = ""
    val header = "{\n\"type\": \"struct\",\n\"fields\": ["
    val footer = "]\n}"
    var jsonData = ""

    for(i <- 0 until readcsvtojson.length) {
      val splitInput = readcsvtojson(i).split(",")
      jsonData = jsonData + "{\n\t\t\"name\": \""+splitInput(0)+"\",\n\t\t\"type\": \""+splitInput(1)+"\",\n\t\t\"nullable\": true,\n\t\t\"metadata\": {\n\t\t\t\"width\": " + splitInput(3) + ",\n\t\t\t\"generated\": false\n\t\t}\n\t}"
      if (i != readcsvtojson.length-1)  jsonData = jsonData + ","
    }
    finalOutput = header + jsonData +footer

    //println(finalOutput)

    val writer = new PrintWriter(new File("C:\\Users\\ABVP190\\Desktop\\Barclays ABSA\\BRAVO\\output_BLG2_20170721_ABSA_29345\\ExperianSchema_13107_struct.json"))

    writer.write(finalOutput)

  }




}


