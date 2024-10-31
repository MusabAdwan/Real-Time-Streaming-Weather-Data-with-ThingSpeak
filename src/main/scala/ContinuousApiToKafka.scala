import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger
import okhttp3.{OkHttpClient, Request}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import java.util.Properties
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.client.RequestBuilding._
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.ActorMaterializer
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

import scala.concurrent.duration._
import scala.concurrent.Future
import scala.util.{Failure, Success}
object ContinuousApiToKafka {
  implicit val system: ActorSystem = ActorSystem("ApiToKafkaSystem")
  implicit val materializer: ActorMaterializer = ActorMaterializer()
  import system.dispatcher // for execution context

  def main(args: Array[String]): Unit = {
    val channelId = "12397" // Replace with your channel ID
    val apiUrl = s"https://api.thingspeak.com/channels/12397/feeds.json"
    val kafkaTopic = "thingspeak" // Replace with your Kafka topic
    val kafkaBroker = "localhost:9092" // Replace with your Kafka broker address

    // Create Kafka producer
    val producerProps = new java.util.Properties()
    producerProps.put("bootstrap.servers", kafkaBroker)
    producerProps.put("key.serializer", classOf[StringSerializer].getName)
    producerProps.put("value.serializer", classOf[StringSerializer].getName)

    val producer = new KafkaProducer[String, String](producerProps)

    // Method to fetch data from API and send to Kafka
    def fetchDataAndSendToKafka(): Unit = {
      val responseFuture: Future[akka.http.scaladsl.model.HttpResponse] = Http().singleRequest(Get(apiUrl))

      responseFuture.flatMap { response =>
        Unmarshal(response.entity).to[String] // Assuming the response is in String format
      }.onComplete {
        case Success(data) =>
          // Send data to Kafka
          val record = new ProducerRecord[String, String](kafkaTopic, data)
          producer.send(record)

          println(s"Data sent to Kafka topic $kafkaTopic: $data")

        case Failure(exception) =>
          println(s"Failed to fetch data: ${exception.getMessage}")
      }
    }

    // Schedule the API call every 100 minutes
    system.scheduler.scheduleWithFixedDelay(initialDelay = 0.seconds, delay = 100.minutes)(() => fetchDataAndSendToKafka())

    // Ensure the application keeps running
    println("Scheduled API calls every 100 minutes. Press ENTER to exit.")
    scala.io.StdIn.readLine()

    // Clean up
    producer.close()
    system.terminate()
  }


}