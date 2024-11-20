//import the necessary libraries (log4j,spark.sql,spark ml,util,http,akka stream,actor,json, concurrent
import org.apache.log4j.BasicConfigurator
import org.apache.log4j.varia.NullAppender
import org.apache.spark.sql.{DataFrame, SparkSession, functions => F}
import org.apache.spark.sql.functions.{col, _}
import org.apache.spark.ml.tuning.CrossValidatorModel
import org.apache.spark.sql.types._

import scala.util.{Failure, Success}
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.stream.ActorMaterializer
import spray.json._

import scala.concurrent.ExecutionContextExecutor
import org.apache.spark.sql.streaming.Trigger

object KafkaConsumer  extends DefaultJsonProtocol{

  def main(args: Array[String]): Unit = {
    case class ReceivedData(data: String)
    implicit val receivedDataFormat = jsonFormat1(ReceivedData)
    val nullAppender = new NullAppender
    BasicConfigurator.configure(nullAppender)

    implicit val system: ActorSystem = ActorSystem("app-b-system")
    implicit val materializer: ActorMaterializer = ActorMaterializer()
    implicit val executionContext: ExecutionContextExecutor = system.dispatcher
    //  target URL for the main server to send the output to
    val mainServerUrl = "http://localhost:8081/receive-data"
    // Initialize Spark session and configuration
    val spark = SparkSession.builder()
      .appName("NetworkWordCount")//Set spark app name
      .master("local[*]") // Run Spark locally using all available
      .config("spark.mongodb.output.uri", "mongodb://localhost:27017/WeatherRecommendation.Grouped Readings")//mongodb storage
      .getOrCreate()//create the spark session
    spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

    // Load the saved model
    val modelPath = "models/WeatherActivityModel"//path of the saved model
    val model = CrossValidatorModel.load(modelPath) // Load the saved model
    // Map numeric predictions back to activity names
    val activityMap = Map(
      0.0 -> "Stay Home", // activity: 0 -> Stay Home
      1.0 -> "Walking", // activity: 1 -> Walking
      2.0 -> "Running", // activity: 2 -> Running
      3.0 -> "Picnicking", // activity: 3 -> Picnicking
      4.0 -> "Reading Outdoors", // activity: 4 -> Reading Outdoors
      5.0 -> "Barbecue", // activity: 5 -> Barbecue
      6.0 -> "Tennis", // activity: 6 -> Kite Flying
      7.0 -> "Yoga Outdoors", // activity: 7 -> Tennis
      8.0 -> "you are free", //activity : 8 -> Yoga Outdoors
    )
    // Read the data from Kafka
    def readFromKafka(topic: String, maxOffsetsPerTrigger: Int, spark: SparkSession): DataFrame = {
      spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", maxOffsetsPerTrigger)
        .load()
    }

    // Parse the CSV data (split by commas)
    def parseKafkaData(kafkaData: DataFrame): DataFrame = {
      val kafkaData1 = kafkaData.selectExpr("CAST(value AS STRING) as message")

      kafkaData1.select(
        F.split(F.col("message"), ",").getItem(0).alias("Station"),
        to_timestamp(F.split(F.col("message"), ",").getItem(1), "yyyy-MM-dd HH:mm:ss.SSS").alias("created_at"),
        F.split(F.col("message"), ",").getItem(2).cast(FloatType).alias("Wind Direction "),
        F.split(F.col("message"), ",").getItem(3).cast(FloatType).alias("Wind Speed (mph)"),
        F.split(F.col("message"), ",").getItem(4).cast(FloatType).alias("% Humidity"),
        F.split(F.col("message"), ",").getItem(5).cast(FloatType).alias("Temperature (F)"),
        F.split(F.col("message"), ",").getItem(6).cast(FloatType).alias("Rain (Inches/minute)"),
        F.split(F.col("message"), ",").getItem(7).cast(FloatType).alias("Pressure (Hg)"),
        F.split(F.col("message"), ",").getItem(8).cast(FloatType).alias("Power Level"),
        F.split(F.col("message"), ",").getItem(9).cast(FloatType).alias("Light Intensity")
      )
    }

    def applyModelAndPredict(parsedData: DataFrame, model: CrossValidatorModel): DataFrame = {
      val predictions = model.transform(parsedData)
      predictions.withColumn("recommended_activity",
        udf((prediction: Double) => activityMap.getOrElse(prediction, "Stay Home")).apply(col("prediction")))
    }
    val rawStream1 = readFromKafka("thingspeak-data", 1, spark)
    val rawStream2 = readFromKafka("thingspeak-data", 10, spark)

    // Parse the data using the helper function
    val parsedData1 = parseKafkaData(rawStream1)
    val parsedData2 = parseKafkaData(rawStream2)

    // Apply model and get predictions
    val predictedActivities1 = applyModelAndPredict(parsedData1, model)
    val predictedActivities2 = applyModelAndPredict(parsedData2, model)

    val finalOutput1 = predictedActivities1
        .select(
          parsedData1("created_at"),
          col("Wind Direction "),
          col("Wind Speed (mph)"),
          col("% Humidity"),
          col("Temperature (F)"),
          col("Rain (Inches/minute)"),
          col("Pressure (Hg)"),
          col("Light Intensity"),
          col("recommended_activity")
        )
    val finalOutput2 = predictedActivities2
      .select(
        parsedData2("created_at"),
        col("Wind Direction "),
        col("Wind Speed (mph)"),
        col("% Humidity"),
        col("Temperature (F)"),
        col("Rain (Inches/minute)"),
        col("Pressure (Hg)"),
        col("Light Intensity"),
        col("recommended_activity")
      )
    val windowedStream = finalOutput2
      .withWatermark("created_at", "1 minute") // Set watermark to handle late data
      .groupBy(
        window(col("created_at"), "5 minutes", "1 minute"), // Define a sliding window
        col("recommended_activity") // Group by an additional column
      )
      .agg(
        avg("Temperature (F)").alias("avg_Temperature (F)"), // Example aggregation
        avg("% Humidity").alias("avg_% Humidity"), // Example aggregation
        max("Wind Speed (mph)").alias("max_wind_speed"),
        max("Pressure (Hg)").alias("max_Pressure (Hg)"),
        max("Light Intensity").alias("max_Light Intensity"),
        // count("*").alias("event_count")
      )
      .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("recommended_activity"),
        col("avg_Temperature (F)"),
        col("avg_% Humidity"),
        col("max_Pressure (Hg)"),
        col("max_Light Intensity")
      )
    // Output the processed data to the console (for debugging purposes)
    val processedStream1 = finalOutput1.writeStream
      .outputMode("append")
      .format("console") // This just outputs the data to the console for debugging
      .trigger(Trigger.ProcessingTime("60 seconds")) // Adjust the trigger as needed
      .start()
     //Adding a continuous count to each reading

     def writeToMongoDB(df: DataFrame, epochId: Long): Unit = {
       val mongoURL = "mongodb://localhost:27017/WeatherRecommendation.Grouped Readings"

       df.write
         .format("mongo")
         .mode("append")
         .option("uri", mongoURL)
         .save()
       println(s"Batch $epochId written to MongoDB successfully.")
     }

    // Output the processed data to MongoDB
        val mongoOutput = windowedStream.writeStream
          .foreachBatch{(df: DataFrame, epochId: Long) =>
            writeToMongoDB(df, epochId) }
          .trigger(Trigger.ProcessingTime("60 seconds")) // Adjust the trigger as needed
          .start()

    val  processedStream2 = finalOutput1.writeStream
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
      }
      .trigger(Trigger.ProcessingTime("60 seconds")) // Adjust the trigger as needed
      .start()


    // Wait for termination of both queries
    processedStream1.awaitTermination()
    mongoOutput.awaitTermination()
    processedStream2.awaitTermination()
  }
}
