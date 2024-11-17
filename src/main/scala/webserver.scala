

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives._
import akka.stream.ActorMaterializer
import scala.concurrent.duration._
import scala.concurrent.ExecutionContextExecutor
import java.util.concurrent.atomic.AtomicReference
import scala.io.StdIn
import spray.json._
import akka.http.scaladsl.model.StatusCodes

object webserver extends DefaultJsonProtocol {
  // JSON case classes and formats
  case class DataResponse(serverData: String, submittedData: Option[String])
  case class ReceivedData(data: String)
  implicit val serverDataFormat = jsonFormat2(DataResponse)
  implicit val receivedDataFormat = jsonFormat1(ReceivedData)

  def main(args: Array[String]): Unit = {
    implicit val system: ActorSystem = ActorSystem("scala-html-server")
    implicit val materializer: ActorMaterializer = ActorMaterializer()
    implicit val executionContext: ExecutionContextExecutor = system.dispatcher

    // Atomic references to hold submitted data
    val submittedData = new AtomicReference[Option[String]](None)

    // Define routes for HTML page, JSON data, and receiving data from App B
    val route =
      path("") {
        get {
          // Serve the HTML page with input field and data display
          val htmlContent =
            s"""
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>Scala Dynamic Data Display</title>
  <style>
  .data-container {
    display: grid;
    grid-template-columns: repeat(8, 1fr); /* 8 equal-width columns */
    grid-gap: 10px; /* Space between items */
    max-width: 1000px;
    margin: 20px auto;
  }

  .title, .value {
    text-align: center;
  }

  .title {
    font-weight: bold;
  }

  /* Ensure titles and values are properly spaced */
  .data-container p {
    margin: 5px 0;
  }
</style>
  <script>
    // Fetch updated data from server every 1 second
    async function fetchData() {
      const response = await fetch('/data'); // Fetch the data from the /data route
      const result = await response.json();  // Parse the JSON response

      // Assuming result.submittedData is a string (e.g., "100,200,300,5,7,8,90,10")
      const dataArray = result.submittedData ? result.submittedData.split(',') : [];  // Split string into an array

      // Function to populate values in the respective elements
      document.getElementById('windDirection').textContent = dataArray[0] || 'No data submitted yet';
      document.getElementById('windSpeed').textContent = dataArray[1] || 'No data submitted yet';
      document.getElementById('humidity').textContent = dataArray[2] || 'No data submitted yet';
      document.getElementById('temperature').textContent = dataArray[3] || 'No data submitted yet';
      document.getElementById('rain').textContent = dataArray[4] || 'No data submitted yet';
      document.getElementById('pressure').textContent = dataArray[5] || 'No data submitted yet';
      document.getElementById('lightIntensity').textContent = dataArray[6] || 'No data submitted yet';
      document.getElementById('recommendedActivity').textContent = dataArray[7] || 'No data submitted yet';
    }

    // Fetch data every 1 second
    setInterval(fetchData, 1000);
  </script>
</head>
<body onload="fetchData()">
  <h1>Weather Recommendation Activity System</h1>

  <!-- Container for the dynamic data display -->
 <div class="data-container">
  <!-- First Row: Titles -->
  <p class="title"><strong>Wind Direction</strong></p>
  <p class="title"><strong>Wind Speed (mph)</strong></p>
  <p class="title"><strong>% Humidity</strong></p>
  <p class="title"><strong>Temperature (F)</strong></p>

  <p class="title"><strong>Rain (Inches/minute)</strong></p>
  <p class="title"><strong>Pressure (Hg)</strong></p>
  <p class="title"><strong>Light Intensity</strong></p>
  <p class="title"><strong>Recommended Activity</strong></p>

  <!-- Second Row: Values -->
  <p class="value" id="windDirection">Loading...</p>
  <p class="value" id="windSpeed">Loading...</p>
  <p class="value" id="humidity">Loading...</p>
  <p class="value" id="temperature">Loading...</p>

  <p class="value" id="rain">Loading...</p>
  <p class="value" id="pressure">Loading...</p>
  <p class="value" id="lightIntensity">Loading...</p>
  <p class="value" id="recommendedActivity">Loading...</p>
</div>

</body>
</html>


            """
          complete(HttpEntity(ContentTypes.`text/html(UTF-8)`, htmlContent))
        }
      } ~
        path("data") {
          get {
            // Respond with current server data and last submitted data
            val response = DataResponse("Server is running", submittedData.get())
            complete(response.toJson.prettyPrint)
          }
        } ~
        path("receive-data") {
          post {
            entity(as[String]) { jsonData =>
              val parsedData = jsonData.parseJson.convertTo[ReceivedData]
              submittedData.set(Some(parsedData.data))
              println(s"Received data from App B: ${parsedData.data}")
              complete(StatusCodes.OK)
            }
          }
        }

    // Start the server
    val bindingFuture = Http().newServerAt("localhost", 8081).bind(route)
    println("Server online at http://localhost:8081/\nPress RETURN to stop...")

    StdIn.readLine()
    bindingFuture
      .flatMap(_.unbind())
      .onComplete(_ => system.terminate())
  }
}