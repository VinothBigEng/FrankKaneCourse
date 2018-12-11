import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql._
import org.apache.log4j._

import scala.io.Source


object ConstructJsonFileforExperian {


  def main (args :Array[String]) ={


    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder().appName("ConstructJsonFileforExperian").master("local[*]").getOrCreate()

    import spark.implicits._


    val ExperianReadSchemaRDD = spark.sparkContext.textFile("C:\\Users\\ABVP190\\Desktop\\Barclays ABSA\\BRAVO\\output_BLG2_20170721_ABSA_29345\\ExperianSchema_13107_struct.csv").filter(!_.isEmpty)

    case class ExperianSchema(ExpColumnName:String,ExpDataType : String ,Expnullabilty:String,ExpcolLength :Int)


    def mapper(line:String): ExperianSchema = {
      val fields = line.split(',')
      val ExpSchema:ExperianSchema = ExperianSchema(fields(0).toString, fields(1).toString,fields(2).toString, fields(3).toInt)
      return ExpSchema
    }

    val ExperianRDDtoDF = ExperianReadSchemaRDD.map(mapper).map(x => (x.ExpColumnName , x.ExpDataType ,x.Expnullabilty,x.ExpcolLength ))
      .toDF().
      select("*").withColumnRenamed("_1","name" ).withColumnRenamed("_2","type").withColumnRenamed("_3","nullable").withColumnRenamed("_4","width")

    val ExpSchemaTempTable = ExperianRDDtoDF.createOrReplaceTempView("ExperianSchema")



    spark.sql("select  name, type,nullable, struct(Width) as metadata from ExperianSchema")
      .coalesce(1)
      .write.mode("overwrite")
      .json("C:\\Users\\ABVP190\\Desktop\\Barclays ABSA\\BRAVO\\output_BLG2_20170721_ABSA_29345\\ExperianSchemaof13107Struct")









  }

}
