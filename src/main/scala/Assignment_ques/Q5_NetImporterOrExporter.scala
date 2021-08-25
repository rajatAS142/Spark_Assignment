package Assignment_ques

//import all the required Libraries
import org.apache.spark.sql._
import org.apache.log4j._

object NetImporterOrExporter {

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
    val query_tool ="""
        select year,
	        case
    	        when totalImport>totalExport then 'Importer'
              else 'Exporter'
          end as Status
        from
        (
          select i.year as year,sum(i.value) as totalImport,sum(e.value) as totalExport
          from imported i inner join exported e
          on i.HSCode = e.HSCode
          group by i.year
        )
        order by year desc
      """

    println("Year wise importer or exporter:\n")
    val MajorChunk = spark.sql(query_tool)
    MajorChunk.toDF.show()

    spark.stop()
  }
}
