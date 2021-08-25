package Assignment_ques

//import all the required Libraries
import org.apache.spark.sql._
import org.apache.log4j._

object MajorChunk {

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

    //schemaExport.printSchema()
    schemaExport.createOrReplaceTempView("exported")
    schemaImport.createOrReplaceTempView("imported")

    println("Commodity which forms major chunk of Trade:\n")
    val query_tool ="""
          select Commodity,round(sum(value)) as total_trade
          from (select Commodity,value from imported union all select Commodity,value from exported)t
          group by Commodity
          order by sum(value) desc
          limit 1
        """
    val MajorChunk = spark.sql(query_tool)
    MajorChunk.show(false)
    spark.stop()
  }
}
