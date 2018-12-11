  import org.apache.spark.SparkContext
  import org.apache.spark.SparkConf
  import org.apache.spark.sql.SparkSession
  import org.apache.spark.sql._
  import java.util.TimeZone;
  import java.text.SimpleDateFormat
  import java.util.{Date, TimeZone}
  import org.apache.spark.sql.Row

  import org.apache.log4j._



  object ConnectionTry {

    def main(args: Array[String]) {

      Logger.getLogger("org").setLevel(Level.ERROR)

      //Create conf object
      val conf = new SparkConf().setAppName("WordCount").setMaster("local[*]")

      //create spark context object
      val sc = new SparkContext(conf)
      //val spark = SparkSession
      val sqlContext = new org.apache.spark.sql.SQLContext(sc)
      //Check whether sufficient params are supplied

      println("hello")
      //val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      //df.setTimeZone(TimeZone.getTimeZone("UTC+02:00"))
      //val date= new Date(1444291567242l); //java.util.Date = Thu Oct 08 09:06:07 BST 2015
      //val dateString = df.format(date);
      // println(dateString)
      // println(date)

      val jdbcUrl = "jdbc:oracle:thin:DWH_READ_ONLY/absa5487#@//10.6.40.2:1600/dwh.prod.im.absa.co.za"
      val tableName = "ABSA.ACCOUNT_MONTHLY"
      val tableName2 = "ABSA.ACCOUNT_TYPE_CODE"
      val connectionProperties = new java.util.Properties
      connectionProperties.put("driver", "oracle.jdbc.driver.OracleDriver")
      connectionProperties.put("username", "DWH_READ_ONLY")
      connectionProperties.put("password", "absa5487#")
      val table1codeDF = sqlContext.read.jdbc(jdbcUrl, tableName, connectionProperties)


      table1codeDF.printSchema()


      table1codeDF.createOrReplaceTempView("table1code")

      val data = sqlContext.sql("SELECT * FROM table1code where ACCOUNT_STATUS_CODE = 1")
      data.write.mode("overwrite").csv("C:\\Users\\ABVP190\\Desktop\\Barclays ABSA\\BRAVO\\results123")




      //val refcodeDF =  sqlContext.read.jdbc(jdbcUrl, tableName2, connectionProperties)
      //refcodeDF.createOrReplaceTempView("refcode")
      /*val result = sqlContext.sql("SELECT * FROM table1code  Where INFORMATION_DATE = '31-MAY-05'")

      val rddresult = result.rdd


      rddresult.saveAsTextFile("C:\\Users\\ABVP190\\Desktop\\Barclays ABSA\\BRAVO\\results") */
      //val result = sqlContext.sql("FROM table1code SELECT DISTINCT ACTIVITY_TYPE_CODE")
      // LEFT OUTER JOIN refcode ON table1code.ACCOUNT_TYPE_CODE = refcode.ACCOUNT_TYPE_CODE WHERE table1code.ACCOUNT_TYPE_CODE IS NULL")

      //result.show()


    }



}
