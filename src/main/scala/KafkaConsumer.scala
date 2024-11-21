//import the necessary libraries
// (log4j,spark.sql,spark ml,util,http,akka stream,actor,json, concurrent
import org.apache.log4j.BasicConfigurator//for configuring logging
import org.apache.log4j.varia.NullAppender// a logging appender
import org.apache.spark.sql.{DataFrame, SparkSession, functions => F}//including DataFrame, SparkSession,
// and functions (aliased as F) for data manipulation.
import org.apache.spark.sql.functions.{col, _}//col for column selection
import org.apache.spark.ml.tuning.CrossValidatorModel//for model evaluation using cross-validation.
import org.apache.spark.sql.types._// define schemas and data types for DataFrames.
import scala.util.{Failure, Success}//Failure and Success for handling results in Scala's Try constructs
import akka.actor.ActorSystem//creating and managing actors
import akka.http.scaladsl.Http// HTTP requests and handling responses
import akka.http.scaladsl.model._
import akka.stream.ActorMaterializer//required for materializing streams
import spray.json._//JSON serialization and deserialization in Scala

import scala.concurrent.ExecutionContextExecutor//managing execution contexts in concurrent programming
import org.apache.spark.sql.streaming.Trigger//controlling the execution of streaming queries
//object named KafkaConsumer
object KafkaConsumer  extends DefaultJsonProtocol{//DefaultJsonProtocol allows the object
  // to provide JSON serialization and deserialization functionality.
  // Main method
  def main(args: Array[String]): Unit = {
    case class ReceivedData(data: String)//a case class  with a single field 'data' of type String.
    //automatic conversion between JSON and the ReceivedData class
    implicit val receivedDataFormat = jsonFormat1(ReceivedData)
    val nullAppender = new NullAppender//instance of NullAppender from log4j, to discard all log messages.
    BasicConfigurator.configure(nullAppender)// Configure the log4j logging  to use the NullAppender to silence log output.

    // create an ActorSystem named "app-b-system" ,will manage the lifecycle of actors within the application.
    implicit val system: ActorSystem = ActorSystem("app-b-system")
    // It enables the execution of stream processing
    implicit val materializer: ActorMaterializer = ActorMaterializer()
   //ExecutionContextExecutor will allow asynchronous operations
   // to be executed in the context of the created ActorSystem.
    // The dispatcher is responsible for managing thread pools and executing tasks
    implicit val executionContext: ExecutionContextExecutor = system.dispatcher
    // a constant `mainServerUrl` that holds the target URL for the main server
    // to which the application will send output data.
    // a local server running on port 8081 at the endpoint "/receive-data".
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

    // Function to read data from a specified Kafka topic.
  //  Parameters: - topic ,maxOffsetsPerTrigger,spark
    def readFromKafka(topic: String, maxOffsetsPerTrigger: Int, spark: SparkSession): DataFrame = {
      spark.readStream//Spark’s readStream to read data from Kafka
        .format("kafka")    // data source format as Kafka.
        .option("kafka.bootstrap.servers", "localhost:9092") // Kafka bootstrap server address (Kafka broker is running).
        .option("subscribe", topic)// - topic: The name of the Kafka topic to read from.
        .option("startingOffsets", "earliest")  // Configure starting offsets
        .option("maxOffsetsPerTrigger", maxOffsetsPerTrigger)// - maxOffsetsPerTrigger: The maximum number of offsets to read per trigger.
        .load()    // Load the data as a DataFrame.
    }
    // Function to parse the incoming Kafka data from a DataFrame.
    def parseKafkaData(kafkaData: DataFrame): DataFrame = {
      // Select "value" field from the Kafka DataFrame and cast it to a String.
      // This represents the actual message content.
      val kafkaData1 = kafkaData.selectExpr("CAST(value AS STRING) as message")
      kafkaData1.select(// Parse the CSV data (split by commas)
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
    // Function to apply a machine learning model to the parsed data and generate predictions.
    // Parameters: - parsedData, model
    def applyModelAndPredict(parsedData: DataFrame, model: CrossValidatorModel): DataFrame = {
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
      //model to transform the parsed data and generate predictions.
      val predictions = model.transform(parsedData)
      // Add a new column "recommended_activity" to the predictions DataFrame.
      predictions.withColumn("recommended_activity",
        // Create a UDF that retrieves the recommended activity based on the prediction.
        udf((prediction: Double) => activityMap.getOrElse(prediction, "Stay Home")).apply(col("prediction")))
    }
    // Read data from the Kafka topic "thingspeak-data" with specified offsets.
    val rawStream1 = readFromKafka("thingspeak-data", 1, spark)
    val rawStream2 = readFromKafka("thingspeak-data", 10, spark)

    // Parse the raw Kafka data using the helper function
    val parsedData1 = parseKafkaData(rawStream1)
    val parsedData2 = parseKafkaData(rawStream2)
    // Apply the machine learning model and get predictions
    val predictedActivities1 = applyModelAndPredict(parsedData1, model)
    val predictedActivities2 = applyModelAndPredict(parsedData2, model)
    // Create finaloutput1 DataFrame from the first set of predicted activities.
    // This DataFrame selects specific columns for the final output.
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
    // Create finaloutput2 DataFrame from the first set of predicted activities.
    // This DataFrame selects specific columns for the final output.
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
    // A windowed stream is created from the finalOutput2 DataFrame.
    // This enables processing of data in time-based windows.
    val windowedStream = finalOutput2
      //  a watermark is set on the "created_at" column to handle late data.
      .withWatermark("created_at", "1 minute") // Set watermark to handle late data
      // A sliding window of 5 minutes, sliding every 1 minute.
      // This allows for aggregating data over the last 5 minutes.
      .groupBy(
        window(col("created_at"), "5 minutes", "1 minute"), // Define a sliding window
        col("recommended_activity") // Group by an additional column
      )
      .agg(
        // Calculation of average temperature over the window.
        avg("Temperature (F)").alias("avg_Temperature (F)"),
        // Calculation of average humidity over the window.
        avg("% Humidity").alias("avg_% Humidity"),
        // Maximum wind speed recorded in the window.
        max("Wind Speed (mph)").alias("max_wind_speed"),
        // Maximum pressure recorded in the window.
        max("Pressure (Hg)").alias("max_Pressure (Hg)"),
        // Maximum light intensity recorded in the window.
        max("Light Intensity").alias("max_Light Intensity"),
        count("*").alias("event_count")//a count of events in the window
      )
      .select( // Selection and rename the relevant columns for the windowedStream DataFrame.
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("recommended_activity"),
        col("avg_Temperature (F)"),
        col("avg_% Humidity"),
        col("max_Pressure (Hg)"),
        col("max_Light Intensity")
      )
    // Create a streaming DataFrame from finalOutput1 for processing.
    val processedStream1 = finalOutput1.writeStream
      .outputMode("append") // Set the output mode to "append"
      .format("console") // This just outputs the data to the console for debugging
      .trigger(Trigger.ProcessingTime("60 seconds"))  // Set the trigger to process data every 60 seconds.
      .start() // Start the streaming query.

    // Function to write the DataFrame to MongoDB.
    // Parameters- df, - epochId
    def writeToMongoDB(df: DataFrame, epochId: Long): Unit = {
      // MongoDB connection string and target database/collection.
      val mongoURL = "mongodb://localhost:27017/WeatherRecommendation.Grouped Readings"
       df.write  // Write the DataFrame to MongoDB
         .format("mongo") // Specify the MongoDB format for writing.
         .mode("append")// Append mode
         .option("uri", mongoURL)// MongoDB connection URI.
         .save()// Execute the write operation.
       println(s"Batch $epochId written to MongoDB successfully.") // a confirmation message batch has been written.
    }

      // Output the processed data from the windowed stream to MongoDB.
      // This uses a foreachBatch operation to handle each micro-batch of data.
      val mongoOutput = windowedStream.writeStream
        // Define a foreachBatch operation that processes each micro-batch of data.
        .foreachBatch{(df: DataFrame, epochId: Long) =>
            writeToMongoDB(df, epochId) } // Call the writeToMongoDB function
        // Set the trigger for processing time to 60 seconds.
          .trigger(Trigger.ProcessingTime("60 seconds"))
          .start() // Start the streaming query
    // Create a streaming DataFrame from finalOutput1 for processing.
    // This stream will send data to a specified server in JSON format.
    val  processedStream2 = finalOutput1.writeStream
      .outputMode("append") //append mode
      // Define a foreachBatch operation to process each micro-batch of data.
      .foreachBatch { (batchDF: org.apache.spark.sql.Dataset[org.apache.spark.sql.Row], batchId: Long) =>
        // Convert each row in the batch to a string
        val rowsAsString = batchDF.collect().map(_.mkString(", "))
        val resultString = rowsAsString.mkString("\n")// Combine all rows into a single string with each row on a new line.


        // Prepare JSON data by creating a ReceivedData object and converting it to JSON.
        val jsonData = ReceivedData(resultString).toJson.prettyPrint
        // Create an HTTP entity with the JSON data and specify the content type.
        val requestEntity = HttpEntity(ContentTypes.`application/json`, jsonData)

        // Send the JSON data as HTTP POST
        val request = HttpRequest(
          method = HttpMethods.POST,
          uri = mainServerUrl,//the specified URI
          entity = requestEntity//entity
        )

        // Send the HTTP request and handle the response asynchronously.
        Http().singleRequest(request).onComplete {
          case Success(response) =>
            // a message indicating the batch was sent successfully along with the response status.
          println(s"Sent batch $batchId to server, response: ${response.status}")
            // If the response was not successful, print the response body for debugging.
            if (!response.status.isSuccess()) {
              response.entity.dataBytes.runForeach { byteString =>
                println(s"Response body: ${byteString.utf8String}")
              }
            }
          // Handle the case where the HTTP request fails.
          case Failure(ex) =>
            // a message indicating the failure to send the batch, along with the exception message.
          println(s"Failed to send batch $batchId: ${ex.getMessage}")
        }
      }
      .trigger(Trigger.ProcessingTime("60 seconds"))         // Set the trigger for processing time to 60 seconds.
      .start()// Start the streaming query


    // Wait for termination of queries
    processedStream1.awaitTermination()
    mongoOutput.awaitTermination()
    processedStream2.awaitTermination()
  }
}
