package Assignment_ques

//import all the required Libraries
import org.apache.spark.sql._
import org.apache.log4j._

object TradeGrownOverTime {

  //creating case class for schema of the table
  case class exported(HSCode: Int, Commodity: String, value: Double, country: String, year: Int)
  case class imported(HSCode: Int, Commodity: String, value: Double, country: String, year: Int)

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
      .as[exported]

    val schemaImport = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("/Users/sigmoid/Documents/Spark_Assignment/Data/india-trade-data/2018-2010_import.csv")
      .as[imported]

    schemaExport.createOrReplaceTempView("exported")
    schemaImport.createOrReplaceTempView("imported")

    println("This is how trade between India and any given country grown over time:\n")
    val query_tool ="""
          select country, year, round(sum(value)) as Total_Export
          from (select country,year,value from imported union all select country,year,value from exported)
          group by country, year
          order by country,year
        """
    val MajorChunk = spark.sql(query_tool)
    MajorChunk.toDF.show(100,false)

    spark.stop()
  }
}
