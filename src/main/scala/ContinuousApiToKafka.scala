// Import the necessary libraries and classes
// (Spark SQL classes and functions, Scalaj HTTP, Spark Streaming,
import org.apache.spark.sql.{SparkSession, functions => F}
import scalaj.http._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.receiver.Receiver// Import Receiver class
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}//Import Kafka Producer
import java.util.Properties // Import Properties class for configuration

// Defining an object for the application
object ContinuousApiToKafka {
  def main(args: Array[String]): Unit = {
    // Creating a Spark session
    val spark = SparkSession.builder()
      .appName("ThingSpeak Stream") // Set application name
      .master("local[*]") // Use local mode with all available cores
      .getOrCreate()  //  Spark session creation

    import spark.implicits._// Import implicits for DataFrame operations
    // Creating a StreamingContext with a 15-second batch interval
    val ssc = new StreamingContext(spark.sparkContext, Seconds(15))

    // ThingSpeak channel ID and API endpoint
    val channelId = "12397"
    val apiUrl = s"https://api.thingspeak.com/channels/$channelId/feeds.json"

    // Kafka producer properties
    val kafkaProps = new Properties()
    kafkaProps.put("bootstrap.servers", "localhost:9092")// Setting Kafka server address
    kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")// Setting key serializer
    kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")// Setting value serializer
    // Kafka producer
    val producer = new KafkaProducer[String, String](kafkaProps)  // Kafka producer

    // Kafka topic
    val kafkaTopic = "thingspeak-data"

    // fetch data from ThingSpeak
    def fetchData(): String = { // Method to fetch data
      val response = Http(apiUrl).asString // HTTP GET request to the API
      response.body // Return the response body
    }
    // stream that fetches data every 15 seconds
    val dataStream = ssc.receiverStream(new Receiver[String](org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK_2) {
      override def onStart(): Unit = { // Start method for the receiver
        while (!isStopped) { // Continue fetching until receiver is stopped
          val data = fetchData()  // Fetch data from ThingSpeak
          store(data) // Storing the fetched data
        }
      }
      override def onStop(): Unit = {} // Stop method
    })
    dataStream.foreachRDD { rdd => // Processing each RDD in the stream
      if (!rdd.isEmpty()) { // Checking if the RDD is not empty
        val jsonData = rdd.collect().mkString("\n") // Collecting data and converting it to a single JSON string
        if (jsonData.nonEmpty) { // Proceed if the JSON string is non-empty
          // Create a DataFrame from the JSON string
          val df = spark.read.json(Seq(jsonData).toDS())
          // Explode feeds array to flatten the data
          val explodedDF = df.withColumn("feeds", F.explode($"feeds"))

          // Selecting the desired fields from dataframe
          val resultDF = explodedDF.select(
            $"channel.name".as("channel_name"),// Selecting channel name
            $"feeds.created_at".as("timestamp"),// Selecting humidity
            $"feeds.field1".as("Wind Direction (North = 0 degrees)"),// Selecting wind direction
            $"feeds.field2".as("Wind Speed (mph)"),// Selecting wind speed
            $"feeds.field3".as("Humidity %"),// Selecting humidity
            $"feeds.field4".as("Temperature F"),// Selecting temperature
            $"feeds.field5".as("Rain (Inches/minute)"),// Selecting rain
            $"feeds.field6".as("Pressure (\\\"Hg)"), // Selecting pressure
            $"feeds.field7".as("Power Level (V)"),// Selecting power level
            $"feeds.field8".as("Light Intensity")// Selecting light intensity
          ).withColumn("timestamp", F.to_timestamp($"timestamp")) // Converting string timestamp to TimestampType

          // Send each row to Kafka
          resultDF.collect().foreach { row =>  // Collecting and iterating over each row in the DataFrame
            val key = row.getAs[String]("channel_name") // Getting the channel name as the key
            val value = row.mkString(",") // Converting the entire row to a comma-separated string
            producer.send(new ProducerRecord[String, String](kafkaTopic, key, value))// Send the record to Kafka
          }
        }
      }
    }
    ssc.start()  // Start the streaming context
    ssc.awaitTermination() // Wait for the streaming to finish
    producer.close() // Close the Kafka producer
  }
}