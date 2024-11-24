

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
  case class DataResponse(serverData: String, submittedData: Option[String], submittedData2: Option[String], submittedData3: Option[Long],submittedData4: Option[String])
  case class ReceivedData(data: String)
  case class ReceivedData2(data: String)

  case class ActivityCount(activity: String, count: Long)

  // Define JSON formats
  implicit val serverDataFormat: RootJsonFormat[DataResponse] = jsonFormat5(DataResponse)
  implicit val receivedDataFormat: RootJsonFormat[ReceivedData] = jsonFormat1(ReceivedData)
  implicit val activityCountFormat: RootJsonFormat[ActivityCount] = jsonFormat2(ActivityCount)
  implicit val receivedDataFormat2: RootJsonFormat[ReceivedData2] = jsonFormat1(ReceivedData2)


  def main(args: Array[String]): Unit = {
    implicit val system: ActorSystem = ActorSystem("scala-html-server")
    implicit val materializer: ActorMaterializer = ActorMaterializer()
    implicit val executionContext: ExecutionContextExecutor = system.dispatcher

    // Atomic references to hold submitted data
    val submittedData = new AtomicReference[Option[String]](None)
    var submittedData2  =new AtomicReference[Option[String]](None)// Using Option to hold the data
    var submittedData3  =new AtomicReference[Option[Long]](None)// Using Option to hold the data
    var submittedData4  =new AtomicReference[Option[String]](None)// Using Option to hold the data

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
    /* CSS for data layout */
    .data-container {
    display: grid;
    grid-template-columns: repeat(16, 1fr); /* 16 equal-width columns */
    grid-gap: 10px; /* Space between items */
    max-width: 100%;
    margin: 20px auto;
  }
  .title, .value {
    text-align: center;
  }

  .title {
    font-weight: bold;
  }
  {
  margin-left:3px;
  border-right:2px solid black;
  }

    .data-container p {
    margin: 5px 0;
  }
    canvas {
      max-width: 550px;
      margin: 60px auto;
    }
  </style>
  <!-- Chart.js library -->
  <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
  <script>
    // Set up global variables for each chart
    let charts = {};

    // Initialize the charts for each data type
    function initializeCharts() {
      const ctxWindSpeed = document.getElementById('windSpeedChart').getContext('2d');
      charts.windSpeed = createChart(ctxWindSpeed, 'Wind Speed (mph)');

      const ctxTemperature = document.getElementById('temperatureChart').getContext('2d');
      charts.temperature = createChart(ctxTemperature, 'Temperature (F)');

      const ctxHumidity = document.getElementById('humidityChart').getContext('2d');
      charts.humidity = createChart(ctxHumidity, 'humidity ');

      const ctxRain = document.getElementById('rainChart').getContext('2d');
      charts.rain = createChart(ctxRain, 'rain ');

      const ctxPressure = document.getElementById('pressureChart').getContext('2d');
      charts.pressure = createChart(ctxPressure, 'pressure ');

      const ctxLightIntensity = document.getElementById('lightIntensityChart').getContext('2d');
      charts.lightIntensity = createChart(ctxLightIntensity, 'lightIntensity ');

    }

    // Function to create a new line chart
    function createChart(ctx, label) {
      return new Chart(ctx, {
        type: 'line',
        data: {
          labels: [], // Timestamp labels will be added here
          datasets: [{
            label: label,
            data: [], // Data points will be added here
            fill: false,
            borderColor: 'rgba(75, 192, 192, 1)',
            tension: 0.1
          }]
        },
        options: {
          scales: {
            x: { display: true, title: { display: true, text: 'Time' }},
            y: { display: true, title: { display: true, text: label }}
          }
        }
      });
    }

    // Fetch updated data from server every second
    async function fetchData() {
      const response = await fetch('/data');
      const result = await response.json();
      const dataArray = result.submittedData ? result.submittedData.split(',') : [];
      const activity = result.submittedData2 ;
      const count = result.submittedData3 ;
      const dataArray2 = result.submittedData4 ? result.submittedData4.split(',') : [];

      // Update each displayed value and add data to charts
      updateDisplayedData(dataArray,count,dataArray2);
      updateCharts(dataArray);
    }

    // Display data in HTML
    function updateDisplayedData(dataArray,count,dataArray2) {
      document.getElementById('Time').textContent = dataArray[0] || 'No data';
      document.getElementById('windDirection').textContent = dataArray[1] || 'No data';
      document.getElementById('windSpeed').textContent = dataArray[2] || 'No data';
      document.getElementById('humidity').textContent = dataArray[3] || 'No data';
      document.getElementById('temperature').textContent = dataArray[4] || 'No data';
      document.getElementById('rain').textContent = dataArray[5] || 'No data';
      document.getElementById('pressure').textContent = dataArray[6] || 'No data';
      document.getElementById('lightIntensity').textContent = dataArray[7] || 'No data';
      document.getElementById('recommendedActivity').textContent = dataArray[8] || 'No data';
      document.getElementById('Count').textContent = count || 'No data';
      document.getElementById('window_start').textContent = dataArray2[0] || 'No data';
      document.getElementById('window_end').textContent = dataArray2[1] || 'No data';
      document.getElementById('avg_Temperature (F)').textContent = dataArray2[3] || 'No data';
      document.getElementById('avg_% Humidity').textContent = dataArray2[4] || 'No data';
      document.getElementById('max_Pressure (Hg)').textContent = dataArray2[5] || 'No data';
      document.getElementById('max_Light Intensity').textContent = dataArray2[6] || 'No data';

    }

    // Update the charts with new data
    function updateCharts(dataArray) {
      const timestamp = new Date().toLocaleTimeString();

      // Add data points to each chart if available
      if (charts.windSpeed && dataArray[2]) {
        charts.windSpeed.data.labels.push(timestamp);
        charts.windSpeed.data.datasets[0].data.push(dataArray[2]);
        charts.windSpeed.update();
      }
      if (charts.temperature && dataArray[4]) {
        charts.temperature.data.labels.push(timestamp);
        charts.temperature.data.datasets[0].data.push(dataArray[4]);
        charts.temperature.update();
      }
       if (charts.humidity && dataArray[3]) {
        charts.humidity.data.labels.push(timestamp);
        charts.humidity.data.datasets[0].data.push(dataArray[3]);
        charts.humidity.update();
      }
      if (charts.rain && dataArray[5]) {
        charts.rain.data.labels.push(timestamp);
        charts.rain.data.datasets[0].data.push(dataArray[5]);
        charts.rain.update();
      }
      if (charts.pressure && dataArray[6]) {
        charts.pressure.data.labels.push(timestamp);
        charts.pressure.data.datasets[0].data.push(dataArray[6]);
        charts.pressure.update();
      }
      if (charts.lightIntensity && dataArray[7]) {
        charts.lightIntensity.data.labels.push(timestamp);
        charts.lightIntensity.data.datasets[0].data.push(dataArray[7]);
        charts.lightIntensity.update();
      }
      // Add similar updates for other charts if needed

      // Limit data points to the last 20 for readability
      Object.values(charts).forEach(chart => {
        if (chart.data.labels.length > 20) {
          chart.data.labels.shift();
          chart.data.datasets[0].data.shift();
        }
      });
    }

    // Initialize charts and set up data fetching
    document.addEventListener('DOMContentLoaded', () => {
      initializeCharts();
      setInterval(fetchData, 1000);
    });
  </script>
</head>
<body>
  <h1>Weather Recommendation Activity System</h1>

 <div class="data-container">
  <!-- First Row: Titles -->
  <p class="title"><strong>Time</strong></p>
  <p class="title"><strong>Wind Direction</strong></p>
  <p class="title"><strong>Wind Speed (mph)</strong></p>
  <p class="title"><strong>% Humidity</strong></p>
  <p class="title"><strong>Temperature (F)</strong></p>

  <p class="title"><strong>Rain (Inches/minute)</strong></p>
  <p class="title"><strong>Pressure (Hg)</strong></p>
  <p class="title"><strong>Light Intensity</strong></p>
  <p class="title"><strong>Recommended Activity</strong></p>
  <p class="title"><strong>Activity Count Appearance </strong></p>
  <p class="title"><strong>window_start</strong></p>
  <p class="title"><strong>window_end</strong></p>
  <p class="title"><strong>avg_Temperature (F)</strong></p>
    <p class="title"><strong>avg_% Humidity</strong></p>
  <p class="title"><strong>max_Pressure (Hg)</strong></p>
  <p class="title"><strong>max_Light Intensity</strong></p>

  <!-- Second Row: Values -->
  <p class="value" id="Time">Loading...</p>
  <p class="value" id="windDirection">Loading...</p>
  <p class="value" id="windSpeed">Loading...</p>
  <p class="value" id="humidity">Loading...</p>
  <p class="value" id="temperature">Loading...</p>
  <p class="value" id="rain">Loading...</p>
  <p class="value" id="pressure">Loading...</p>
  <p class="value" id="lightIntensity">Loading...</p>
  <p class="value" id="recommendedActivity">Loading...</p>
  <p class="value" id="Count">Loading...</p>
  <p class="value" id="window_start">Loading...</p>
  <p class="value" id="window_end">Loading...</p>
  <p class="value" id="avg_Temperature (F)">Loading...</p>
   <p class="value" id="avg_% Humidity">Loading...</p>
  <p class="value" id="max_Pressure (Hg)">Loading...</p>
  <p class="value" id="max_Light Intensity">Loading...</p>
</div>

  <!-- Canvas elements for charts -->
  <div class="data-container">
  <canvas id="windSpeedChart" width="600" height="100"></canvas>
  <canvas id="temperatureChart" width="600" height="100"></canvas>
  <canvas id="humidityChart" width="600" height="100"></canvas>
  </div>

   <div class="data-container">
  <canvas id="rainChart" width="600" height="100"></canvas>
  <canvas id="pressureChart" width="600" height="100"></canvas>
  <canvas id="lightIntensityChart" width="600" height="100"></canvas>
 </div>

  <!-- Add more canvases if needed -->
</body>
</html>
\"\"\"


            """
          complete(HttpEntity(ContentTypes.`text/html(UTF-8)`, htmlContent))
        }
      } ~
        path("data") {
          get {
            // Respond with current server data and last submitted data
            val response = DataResponse("Server is running", submittedData.get(),submittedData2.get(), submittedData3.get(),submittedData4.get())
            complete(response.toJson.prettyPrint)
          }
        } ~
        path("receive-data") {
          post {
            entity(as[String]) { jsonData =>
              try {
                val parsedData = jsonData.parseJson.convertTo[ReceivedData]
                submittedData.set(Some(parsedData.data))
                println(s"Received data: ${parsedData.data}")
                complete(StatusCodes.OK)
              } catch {
                case e: Exception =>
                  println(s"Error parsing JSON: ${e.getMessage}")
                  complete(StatusCodes.BadRequest, "Invalid JSON format")
              }
            }
          }
        } ~
        path("receive-data1") {
          post {
            entity(as[String]) { jsonData =>
              try {
                val parsedData = jsonData.parseJson.convertTo[ActivityCount]
                submittedData2.set(Some(parsedData.activity))
                submittedData3.set(Some(parsedData.count))
                println(s"Parsed activity: ${parsedData.activity} with count: ${parsedData.count}")
                complete(StatusCodes.OK)
              } catch {
                case e: Exception =>
                  println(s"Error parsing JSON: ${e.getMessage}")
                  complete(StatusCodes.BadRequest, "Invalid JSON format")
              }
            }
          }
        }~
        path("receive-data2") {
          post {
              entity(as[String]) { jsonData =>
                try {
                  val parsedData = jsonData.parseJson.convertTo[ReceivedData2]
                  submittedData4.set(Some(parsedData.data))
                  println(s"Received data2: ${parsedData.data}")
                  complete(StatusCodes.OK)
                } catch {
                  case e: Exception =>
                    println(s"Error parsing JSON: ${e.getMessage}")
                    complete(StatusCodes.BadRequest, "Invalid JSON format")
              }
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