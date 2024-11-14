
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.log4j.BasicConfigurator
import org.apache.log4j.varia.NullAppender
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.ml.tuning.{CrossValidator, CrossValidatorModel, ParamGridBuilder}
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.stream.ActorMaterializer
import spray.json._
import scala.concurrent.duration._
import scala.concurrent.ExecutionContextExecutor
object KafkaConsumer extends DefaultJsonProtocol {

  def main(args: Array[String]): Unit = {

    case class ReceivedData(data: String)
    implicit val receivedDataFormat = jsonFormat1(ReceivedData)
    val nullAppender = new NullAppender
    BasicConfigurator.configure(nullAppender)

    implicit val system: ActorSystem = ActorSystem("app-b-system")
    implicit val materializer: ActorMaterializer = ActorMaterializer()
    implicit val executionContext: ExecutionContextExecutor = system.dispatcher
    //  target URL for the main server
    val mainServerUrl = "http://localhost:8081/receive-data"
    // Initialize Spark session and configuration
    val spark = SparkSession.builder()
      .appName("NetworkWordCount")
      .master("local[*]")
      .getOrCreate()

    // Load the saved model
    val modelPath = "models/WeatherActivityModel"
    val model = CrossValidatorModel.load(modelPath)

    val ssc = new StreamingContext(spark.sparkContext, Seconds(2))
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean),
      //"max.partition.fetch.bytes" -> 1048576 // Fetch limit
    )
    val topics = Array("thingspeak-data")
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )
    // Define the maximum number of records to process in each micro-batch
    val maxRecordsPerBatch = 1
    stream.foreachRDD { rdd =>
      if (!rdd.isEmpty()) {
        // Extract the values directly from the RDD
        val dataRDD = rdd.map(record => record.value)
        // Limit the number of records
        val limitedRDD = dataRDD.take(maxRecordsPerBatch)
        // Parse the data
        val parsedData = limitedRDD.map { record =>
          val parts = record.split(",")
          (
            parts(0), // Station
            parts(1), // Timestamp
            parts(2).toInt,    // Wind Direction
            parts(3).toDouble, // Wind Speed
            parts(4).toInt,    // Humidity
            parts(5).toDouble, // Temperature
            parts(6).toDouble, // Rain
            parts(7).toDouble, // Pressure
            parts(8).toDouble, // Power Level
            parts(9).toInt     // Light Intensity
          )
        }
        // Convert to DataFrame
        val weatherDF = spark.createDataFrame(parsedData)
          .toDF("Station", "Timestamp", "Wind Direction ", "Wind Speed (mph)",
            "% Humidity", "Temperature (F)", "Rain (Inches/minute)",
            "Pressure (Hg)", "Power_Level", "Light Intensity")
        // process data
        val weatherDFRenamed = weatherDF
          .withColumnRenamed("Wind_Direction", "Wind Direction ")
          .withColumnRenamed("Wind_Speed", "Wind Speed (mph)")
          .withColumnRenamed("Humidity", "% Humidity")
          .withColumnRenamed("Temperature", "Temperature (F)")
          .withColumnRenamed("Rain", "Rain (Inches/minute)")
          .withColumnRenamed("Pressure", "Pressure (Hg)")
          .withColumnRenamed("Light_Intensity", "Light Intensity")
        val updatedDF = weatherDFRenamed.drop("station", "timestamp", "Power_Level")
        // Make predictions using the loaded model
        val predictions = model.transform(updatedDF)
        // Show predictions
        predictions.select("features", "prediction").show(truncate = false)
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
        finalOutput.show(10, truncate = false)
        finalOutput.groupBy(col("recommended_activity")).count().show()
        val rowsAsString: Array[String] = finalOutput.take(10).map(_.mkString(", "))
        val resultString: String = rowsAsString.mkString("\n")
        println(resultString)
        // Prepare JSON data to send to the main server
        val processedData="resultString"
        val jsonData = ReceivedData(resultString).toJson.prettyPrint
        val requestEntity = HttpEntity(ContentTypes.`application/json`, jsonData)

        // Send processed data to the main server
        Http().singleRequest(HttpRequest(
          method = HttpMethods.POST,
          uri = mainServerUrl,
          entity = requestEntity
        )).map { response =>
          println(s"Sent processed data to main server, response: ${response.status}")
      }}
    }
    // Await termination of the query
    ssc.start()
    ssc.awaitTermination()
  }
}