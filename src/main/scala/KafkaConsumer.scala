
import org.apache.log4j.BasicConfigurator
import org.apache.log4j.varia.NullAppender
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.ml.tuning.{CrossValidator, CrossValidatorModel, ParamGridBuilder}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{SparkSession, functions => F}
import scala.util.{Success, Failure}
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.stream.ActorMaterializer
import spray.json._

import scala.concurrent.ExecutionContextExecutor
object KafkaConsumer  extends DefaultJsonProtocol{

  def main(args: Array[String]): Unit = {
    case class ReceivedData(data: String)
    implicit val receivedDataFormat = jsonFormat1(ReceivedData)
    val nullAppender = new NullAppender
    BasicConfigurator.configure(nullAppender)
    // Initialize Spark session and configuration


    implicit val system: ActorSystem = ActorSystem("app-b-system")
    implicit val materializer: ActorMaterializer = ActorMaterializer()
    implicit val executionContext: ExecutionContextExecutor = system.dispatcher
    //  target URL for the main server
    val mainServerUrl = "http://localhost:8081/receive-data"
    val spark = SparkSession.builder()
      .appName("NetworkWordCount")
      .master("local[*]") // Run Spark locally using all available cores
      .getOrCreate()

    import spark.implicits._
    // Load the saved model
    val modelPath = "models/WeatherActivityModel"
    val model = CrossValidatorModel.load(modelPath)

    // Define the schema for the CSV data
    val schema = StructType(Array(
      StructField("Station", StringType, true),
      StructField("Timestamp", StringType, true),
      StructField("Wind Direction ", FloatType, true),
      StructField("Wind Speed (mph)", FloatType, true),
      StructField("% Humidity", FloatType, true),
      StructField("Temperature (F)", FloatType, true),
      StructField("Rain (Inches/minute)", FloatType, true),
      StructField("Pressure (Hg)", FloatType, true),
      StructField("Power Level", FloatType, true),
      StructField("Light Intensity", FloatType, true)
    ))

    // Read the data from Kafka
    val rawStream = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "thingspeak-data")
      .option("startingOffsets", "earliest")
      .option("maxOffsetsPerTrigger", "1")
      .load()

    // Deserialize the Kafka message from byte format to string
    val kafkaData = rawStream.selectExpr("CAST(value AS STRING) as message")

    // Parse the CSV data (split by commas)
    val parsedData = kafkaData.select(
      F.split(F.col("message"), ",").getItem(0).alias("Station"),
      F.split(F.col("message"), ",").getItem(1).alias("Timestamp"),
      F.split(F.col("message"), ",").getItem(2).cast(FloatType).alias("Wind Direction "),
      F.split(F.col("message"), ",").getItem(3).cast(FloatType).alias("Wind Speed (mph)"),
      F.split(F.col("message"), ",").getItem(4).cast(FloatType).alias("% Humidity"),
      F.split(F.col("message"), ",").getItem(5).cast(FloatType).alias("Temperature (F)"),
      F.split(F.col("message"), ",").getItem(6).cast(FloatType).alias("Rain (Inches/minute)"),
      F.split(F.col("message"), ",").getItem(7).cast(FloatType).alias("Pressure (Hg)"),
      F.split(F.col("message"), ",").getItem(8).cast(FloatType).alias("Power Level"),
      F.split(F.col("message"), ",").getItem(9).cast(FloatType).alias("Light Intensity")
    )

    val updatedDF = parsedData.drop("Station", "Timestamp", "Power Level")
    // Make predictions using the loaded model
    val predictions = model.transform(updatedDF)

    // Map numeric predictions back to activity names
    val activityMap = Map(
      0.0 -> "Stay Home", // activity: 0 -> Stay Home
      1.0 -> "Walking", // activity: 1 -> Walking
      2.0 -> "Running", // activity: 2 -> Running
      3.0 -> "Picnicking", // activity: 3 -> Picnicking
      4.0 -> "Reading Outdoors", // activity: 4 -> Reading Outdoors
      5.0 -> "Barbecue", // activity: 5 -> Barbecue
      6.0 -> "Kite Flying", // activity: 6 -> Kite Flying
      7.0 -> "Tennis", // activity: 7 -> Tennis
      8.0 -> "Yoga Outdoors", //activity : 8 -> Yoga Outdoors
      9.0 -> "you are free" // activity: 9 -> No specific recommendation, free to choose
    )
    val predictedActivities = predictions.withColumn("recommended_activity",
      udf((prediction: Double) => activityMap.getOrElse(prediction, "Stay Home")).apply(col("prediction")))
    // Select relevant columns for the final output: Including original weather data and the recommended activity
    val finalOutput = predictedActivities
      .select("Wind Direction ", "Wind Speed (mph)", "% Humidity", "Temperature (F)", "Rain (Inches/minute)",
        "Pressure (Hg)", "Light Intensity", "recommended_activity")

    // Output the processed data to the console (for debugging purposes)
    val processedStream1 = finalOutput.writeStream
      .outputMode("append")
      .format("console") // This just outputs the data to the console for debugging
      .start()

    val  processedStream2 = finalOutput.writeStream
      .outputMode("append") // or "update" depending on your requirement
      .foreachBatch { (batchDF: org.apache.spark.sql.Dataset[org.apache.spark.sql.Row], batchId: Long) =>
        // Convert each row in the batch to a string
        val rowsAsString = batchDF.collect().map(_.mkString(", "))
        val resultString = rowsAsString.mkString("\n")

        // Prepare JSON data
        val jsonData = ReceivedData(resultString).toJson.prettyPrint
        val requestEntity = HttpEntity(ContentTypes.`application/json`, jsonData)

        // Send the JSON data as HTTP POST
        val request = HttpRequest(
          method = HttpMethods.POST,
          uri = mainServerUrl,
          entity = requestEntity
        )

        // Send the request and handle the response
        Http().singleRequest(request).onComplete {
          case Success(response) =>
            println(s"Sent batch $batchId to server, response: ${response.status}")
            if (!response.status.isSuccess()) {
              response.entity.dataBytes.runForeach { byteString =>
                println(s"Response body: ${byteString.utf8String}")
              }
            }
          case Failure(ex) =>
            println(s"Failed to send batch $batchId: ${ex.getMessage}")
        }
      } .start()


    // Wait for termination of both queries
    processedStream1.awaitTermination()
    processedStream2.awaitTermination()
  }
}