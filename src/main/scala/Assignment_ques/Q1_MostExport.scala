package Assignment_ques

//import all the required Libraries
import org.apache.spark.sql._
import org.apache.log4j._

object MostExport1 {

  //creating case class for schema of the table
  case class export(HSCode: Int, Commodity: String, value: Double, country: String, year: Int)
  def main(args: Array[String]) {
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Use SparkSession interface
    val spark = SparkSession
      .builder
      .appName("SparkSql")
      .master("local[*]")
      .getOrCreate()

    // Load each line of the source data into an Dataset
    import spark.implicits._
    val schemaExport = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("/Users/sigmoid/Documents/Spark_Assignment/Data/india-trade-data/2018-2010_export.csv")
      .as[export]

    //schemaExport.printSchema()
    schemaExport.createOrReplaceTempView("export")
    println("India export the most in given year:\n")
    val query_tool ="""
          select Commodity,year
          from (select sum(value) as total_export,year,Commodity from export group by Commodity,year order by sum(value) Desc)
          limit 1
        """
    val MostExport = spark.sql(query_tool) //quering the
    MostExport.show(false) //Printing the result
    spark.stop()
  }
}