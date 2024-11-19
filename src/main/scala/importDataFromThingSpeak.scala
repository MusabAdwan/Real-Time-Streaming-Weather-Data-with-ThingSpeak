import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.HttpRequest
import akka.stream.scaladsl.{Sink, Source}
import akka.util.ByteString
import com.github.tototoshi.csv._
import spray.json._
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success}
import java.io.File
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

// JSON parsing with spray-json
object JsonProtocol extends DefaultJsonProtocol {
  implicit val feedFormat: RootJsonFormat[Feed] = jsonFormat10(Feed)
  implicit val responseFormat: RootJsonFormat[ApiResponse] = jsonFormat1(ApiResponse)
}

case class Feed(created_at: String, entry_id: Int, field1: Option[String], field2: Option[String], field3: Option[String], field4: Option[String], field5: Option[String], field6: Option[String], field7: Option[String], field8: Option[String])

case class ApiResponse(feeds: Seq[Feed])

object ThingspeakToCSV extends App {
  implicit val system: ActorSystem = ActorSystem("ThingspeakToCSVSystem")
  import JsonProtocol._

  val url = "https://api.thingspeak.com/channels/12397/feeds.json"
  val interval = 100.minutes
  val csvFile = new File("C:\\Users\\DELL\\IdeaProjects\\final project\\data\\thingspeak_data100min.csv")
  val dateFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")

  // Initialize CSV writer
  val writer = CSVWriter.open(csvFile, append = true)

  // Set up Akka Source to poll every 3 minutes
  val source = Source.tick(0.seconds, interval, url)

  source
    .mapAsync(1) { url =>
      Http().singleRequest(HttpRequest(uri = url)).flatMap { response =>
        response.entity.dataBytes.runFold(ByteString.empty)(_ ++ _).map(_.utf8String)
      }
    }
    .map { jsonStr =>
      jsonStr.parseJson.convertTo[ApiResponse]
    }
    .map { apiResponse =>
      val data = apiResponse.feeds.map { feed =>
        Seq(
          feed.created_at,
          feed.entry_id.toString,
          feed.field1.getOrElse(""),
          feed.field2.getOrElse(""),
          feed.field3.getOrElse(""),
          feed.field4.getOrElse(""),
          feed.field5.getOrElse(""),
          feed.field6.getOrElse(""),
          feed.field7.getOrElse(""),
          feed.field8.getOrElse("")
        )
      }
      writer.writeAll(data) // Write to CSV
      println(s"Data written at ${LocalDateTime.now.format(dateFormatter)}")
    }
    .runWith(Sink.ignore)
    .onComplete {
      case Success(_) =>
        println("Stream completed successfully.")
        writer.close()
        system.terminate()
      case Failure(exception) =>
        println(s"Stream failed with exception: $exception")
        writer.close()
        system.terminate()
    }
}
