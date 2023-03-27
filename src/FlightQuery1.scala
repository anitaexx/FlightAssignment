import org.apache.spark.sql.{DataFrame, SparkSession}

object FlightQuery1 {
  def inputCSV(spark: SparkSession, filePath: String): DataFrame =
    spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(filePath)

  def inputFlights(spark: SparkSession, filePath: String): DataFrame = {
    inputCSV(spark, filePath)
  }

  def inputPassengers(spark: SparkSession, filePath: String): DataFrame = {
    inputCSV(spark, filePath)
  }

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("FlightQuery")
      .master("local")
      .getOrCreate()

    try {
      //Importing datasets
      val flights = inputFlights(spark, "Data/flightData.csv")
      val passengers = inputPassengers(spark, "Data/passengers.csv")

      //Creating views
      flights.createOrReplaceTempView("flyView")
      passengers.createOrReplaceTempView("passView")

      //SQL Queries
      val q1 = spark.sql("SELECT MONTH(date) as month, count(*) as Number_of_flights from " +
        "flyView group by MONTH(date) order by MONTH(date);")


      //Showing the query for question 1 in the console
      q1.show()


    } catch {
      case e: Exception =>  e.printStackTrace()
        throw new Exception("Error in main function, print to console failed.")

  } finally {
    spark.stop()

  }
}

}

