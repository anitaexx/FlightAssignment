import java.io.FileWriter
import com.opencsv.CSVWriter
import com.github.tototoshi.csv._
import java.io.File
import java.time.LocalDate
import java.time.format.DateTimeFormatter

object FlightQuery4 {
  private case class Flights1(passengerId: Int, flightId: Int, from: String, to: String, date: LocalDate)

  def main(args: Array[String]): Unit = {
    try {
      val fileIn = new File("Data/flightData.csv")
      val reader = CSVReader.open(fileIn)
      val flightList: List[Flights1] = reader
        .allWithHeaders()
        .map(row => Flights1(row("passengerId").toInt, row("flightId").toInt, row("from"),
          row("to"), LocalDate.parse(row("date"), DateTimeFormatter.ISO_LOCAL_DATE)
        ))

      reader.close()

      //Question 4
      def PassengersCombo(flightList: List[Flights1]): Seq[(Int, Int, Int)] = {
        flightList
          .groupBy(_.flightId)
          .flatMap { case (_, flightList) =>
            flightList.map(_.passengerId).combinations(2).toList
          }
          .groupBy(identity).view.mapValues(_.size)
          .toSeq
          .filter { case (_, count) => count > 3 }
          .flatMap { case (comboIds, count) =>
            val List(passengerId1, passengerId2) = comboIds.sorted
            List((passengerId1, passengerId2, count))
          }
          .sortBy(_._3)(Ordering.Int.reverse)
          .toList
      }

      //Export to csv
      val fileOut = new File("Data/Question4.csv")
      val writer = new FileWriter(fileOut)
      val csvWriter = new CSVWriter(writer)
      val output = PassengersCombo(flightList)
      val header = Array("PassengerId1", "PassengerId2", "Number_Of_Flights_Together")
      csvWriter.writeNext(header)
      output.foreach(row => {
        val passengerId1 = row._1.toString
        val passengerId2 = row._2.toString
        val count = row._3.toString
        val insert = List(passengerId1, passengerId2, count).toArray
        csvWriter.writeNext(insert)
      })
      csvWriter.close()
      writer.close()
    }catch {
      case e: Exception => println("Error Message: " + e.getMessage)
  } finally {
    println("CSV file created")
  }


}
}


