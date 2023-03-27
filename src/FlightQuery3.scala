import com.github.tototoshi.csv._
import com.opencsv.CSVWriter
import java.io.{File, FileWriter}
import java.time.LocalDate
import java.time.format.DateTimeFormatter

object FlightQuery3 {
  private case class Flights(passengerId: Int, flightId: Int, from: String, to: String, date: LocalDate)
  //Import the dataset
  def main(args: Array[String]): Unit = {
    try {
      val file = new File("Data/flightData.csv")
      val reader = CSVReader.open(file)
      val flights: List[Flights] = reader
        .allWithHeaders()
        .map(row => Flights(row("passengerId").toInt, row("flightId").toInt, row("from"),
          row("to"), LocalDate.parse(row("date"), DateTimeFormatter.ISO_LOCAL_DATE)
        ))


      //question 3
      def LongestRunNoUK(passengerId: Int, flights: List[Flights]): Int = {
        flights
          .filter(flight => flight.passengerId == passengerId)
          .groupBy(_.from)
          .values
          .foldLeft((Set[String](), 0)) {
            case ((goingTo, count), flights) =>
              val to = flights.filter(_.to != "UK").map(_.to).toSet
              if (goingTo.isEmpty || goingTo.last == "UK") {
                (to, count)
              } else {
                val newTo = to.diff(goingTo)
                (goingTo ++ newTo, count + newTo.size)
              }
          }
          ._2

      }

      //Calculating longest run for every passenger
      val noUK = flights
        .map(_.passengerId)
        .distinct
        .map(passengerID => (passengerID, LongestRunNoUK(passengerID, flights)))
        .sortBy(_._2)(Ordering.Int.reverse)

      //Printing to CSV
      val fileOut = new File("Data/Question3.csv")
      val writer = new FileWriter(fileOut)
      val csvWriter = new CSVWriter(writer)

      try {
        csvWriter.writeNext(Array("Passenger ID", "Longest Run"))
        noUK.foreach { case (passengerId, longestRun) =>
          csvWriter.writeNext(Array(passengerId.toString, longestRun.toString))
        }
      } finally {
        csvWriter.close()
      }

    } catch {
      case e: Exception => println("Error Message: " + e.getMessage)
    } finally {
      println("CSV file created")
    }
  }

}
