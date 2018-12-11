
import org.apache.spark.sql._
import org.apache.log4j._
//import org.databricks.spark.
import org.apache.spark.SparkContext

object EmiratesDataLoad {

  def main (args :Array[String]): Unit = {
   // Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .appName("EmiratesDataLoad")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///C:/temp")
      .getOrCreate()


    import spark.implicits._

    //First way of reading text file
    //val readAirportsRdd = spark.sparkContext.textFile("C:\\Users\\ABVP190\\Desktop\\Emirates\\airports.csv").filter(!_.isEmpty)

    //val readCarriersRdd = spark.sparkContext.textFile("C:\\Users\\ABVP190\\Desktop\\Emirates\\carriers.csv").filter(!_.isEmpty)

    //val readplanedataRdd = spark.sparkContext.textFile("C:\\Users\\ABVP190\\Desktop\\Emirates\\plane-data.csv").filter((!_.isEmpty))


/*
    case class airportsSchema(iata:String,airport : String,city:String , state:String , country: String, lat:String, longtitude :String)
    case class carriersSchema (code:String, Desc:String)
    //case class plandateSchema(tailnum:String,plantype:String,manufacturer:String, issue_date: String ,model:String,status:String, aircraft_type:String,engine_type:String,year:String)



    def mapaairports (Line:String): airportsSchema = {
     val fields =  Line.split(",")
      val AirportsSplitdata:airportsSchema = airportsSchema(fields(0).trim(), fields(1),fields(2).trim(),fields(3).trim(),fields(4).trim(),fields(5).trim(),fields(6).trim())
      return AirportsSplitdata
    }

    def mapcarriers(Line:String):carriersSchema= {
      val fields = Line.split(",")
      val CarriersSplitdata:carriersSchema = carriersSchema(fields(0).trim(), fields(1).trim())
      return CarriersSplitdata
    }

    /*def mapplanedata(Line:String):plandateSchema= {
      val fields = Line.split(",")
      val PlanedateSplitdata:plandateSchema = plandateSchema(fields(0).trim(),fields(1).trim(),fields(2).trim(),fields(3).trim(),fields(4).trim(),fields(5).trim(),fields(6).trim(),fields(7).trim(),fields(8).trim())
    return PlanedateSplitdata

    }*/

    //val AirportsDF = readAirportsRdd.map(mapaairports).map(aiportdata => (aiportdata.iata,aiportdata.airport,aiportdata.city,aiportdata.state,aiportdata.country,aiportdata.lat,aiportdata.longtitude)).toDF().show()

    //val CarriersDF = readCarriersRdd.map(mapcarriers).map(carrierdata => (carrierdata.code,carrierdata.Desc)).toDF().show()
*/

    //Second  Way of reading text file
    val readAirportDF = spark.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema","true")
      .load("C:\\Users\\ABVP190\\Desktop\\Emirates\\airports.csv").dropDuplicates()

    val readCarriersDF = spark.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema","true")
      .load("C:\\Users\\ABVP190\\Desktop\\Emirates\\carriers.csv").dropDuplicates()

    val readPlanedateDFtmp = spark.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("C:\\Users\\ABVP190\\Desktop\\Emirates\\plane-data.csv")
    //planedatedf.show()
    val readPlanedateDF = readPlanedateDFtmp.na.fill("e",Seq("blank")).dropDuplicates()
    //df2.printSchema()

    val readOTPDF = spark.read.format("com.databricks.spark.csv")
      .option("header" ,"true")
      .option("inferSchema", "true")
      .load("C:\\Users\\ABVP190\\Desktop\\Emirates\\OTP\\*").dropDuplicates()


       //readAirportDF.show(10)
   //readCarriersDF.show(10)
   //readPlanedateDF.show(10)
   //readOTPDF.show(10)

    //write Airport to parquet and avro
//    readAirportDF.write.mode("overwrite").parquet("C:\\Users\\ABVP190\\Desktop\\Emirates\\AirportParquetFile")

  //  readAirportDF.write.format("com.databricks.spark.avro").save("C:\\Users\\ABVP190\\Desktop\\Emirates\\AirportAvroFile")

       //write Carriers to parquet and avro
  //  readCarriersDF.write.mode("overwrite").parquet("C:\\Users\\ABVP190\\Desktop\\Emirates\\CarriersParquetFile")
    //  readCarriersDF.write.format("com.databricks.spark.avro").save("C:\\Users\\ABVP190\\Desktop\\Emirates\\CarriersAvroFile")
    // Write planedata to parquet and avro

    //readPlanedateDF.write.mode("overwrite").parquet("C:\\Users\\ABVP190\\Desktop\\Emirates\\PlaneDataParquetFile")
    //  readPlanedateDF.write.format("com.databricks.spark.avro").save("C:\\Users\\ABVP190\\Desktop\\Emirates\\PlanedataAvroFile")

    // Write planedata to parquet and avro
//    readOTPDF.write.mode("overwrite").parquet("C:\\Users\\ABVP190\\Desktop\\Emirates\\OTPParquetFile")

    //  readOTPDF.write.format("com.databricks.spark.avro").save("C:\\Users\\ABVP190\\Desktop\\Emirates\\OTPAvroFile")


    //Write OTP

/*    readOTPDF.createOrReplaceTempView("OTPHive")
    readCarriersDF.createOrReplaceTempView("CarriersHive")
    readPlanedateDF.createOrReplaceTempView("PlanedataHive")
    readAirportDF.createOrReplaceTempView("airportsHive")



//min(ArrDelay) as minTime ,count(*) as count, UniqueCarrier
/*    spark.sql("select min(ArrDelay) as Mintime, min (DepDelay) as Mindept,count(*) as Count, UniqueCarrier ,C1.Code,c1.Description from OTPHive p1 inner join CarriersHive C1 where p1.UniqueCarrier = C1.Code group by " +
      " p1.UniqueCarrier ,C1.Code, C1.Description order by Count desc" ).show()
*/


   /* spark.sql("select max(ArrDelay) as Maxtime, max(DepDelay) as maxdept,count(*) as Count, UniqueCarrier ,C1.Code,c1.Description from OTPHive p1 inner join CarriersHive C1 where p1.UniqueCarrier = C1.Code group by " +
      " p1.UniqueCarrier ,C1.Code, C1.Description order by Count asc" ).show()
*/
   spark.sql("select max(ArrDelay) as Maxtime, max(DepDelay) as maxdept,count(*) as Count, Year,Month,DayofMonth,DayOfWeek from OTPHive p1 inner join CarriersHive C1 where p1.UniqueCarrier = C1.Code group by " +
      " p1.Year,p1.Month,p1.DayofMonth,p1.DayOfWeek order by Count asc" ).show()


    //spark.sql(" select max(ArrDelay) as Maxtime, max(DepDelay) as maxdept,count(*) as Count, Year,Month,DayofMonth,DayOfWeek from OTPHive group by Year, Month,DayofMonth,DayOfWeek order by Count asc").show(10)








    //spark.sql("select max(ArrDelay) as Maxtime, max(DepDelay) as maxdept,count(*) as Count, UniqueCarrier ,C1.tailnum,c1.issue_date ,c1.year as yearmanufact from OTPHive p1 inner join PlanedataHive C1 where p1.TailNum = C1.TailNum and C1.year is not null and DepDelay is not null and ArrDelay is not null group by " +
      //" p1.UniqueCarrier ,C1.tailnum, C1.issue_date, c1.year  order by Count, c1.year asc" ).show(200)


*/

  }

}
