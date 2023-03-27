import org.apache.spark.sql.{DataFrame, SparkSession}

object FlightQuery2 {

  def inputCSV(spark: SparkSession, filePath: String) : DataFrame =
    spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(filePath)

  def inputFlights(spark: SparkSession, filePath: String) : DataFrame = {
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

      //SQL Query
      val q2 = spark.sql("SELECT flyView.passengerId, COUNT(*) as Number_Of_Flights, p.firstName," +
        "p.lastName FROM flyView JOIN passView p ON flyView.passengerId = p.passengerId GROUP BY flyView.passengerId," +
        " p.firstName, p.lastName ORDER BY Number_Of_Flights DESC LIMIT 100;"
      )

      //Showing the query for questions 2 in the console
      q2.show(100)
    } catch {
      case e: Exception => e.printStackTrace()
        throw new Exception("Error in main function, print to console failed.")

    } finally {
      spark.stop()

    }

  }
}





