import org.apache.spark.sql.{SparkSession, functions => F}
import org.apache.spark.sql.functions._
import scalaj.http._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.receiver.Receiver
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import java.util.Properties

object ContinuousApiToKafka {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("ThingSpeak Stream")
      .master("local[*]")
      .getOrCreate()
    import spark.implicits._
    val ssc = new StreamingContext(spark.sparkContext, Seconds(15))

    // ThingSpeak channel ID and API endpoint
    val channelId = "12397"
    val apiUrl = s"https://api.thingspeak.com/channels/$channelId/feeds.json"

    // Kafka producer properties
    val kafkaProps = new Properties()
    kafkaProps.put("bootstrap.servers", "localhost:9092")
    kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    // Kafka producer
    val producer = new KafkaProducer[String, String](kafkaProps)

    // Kafka topic
    val kafkaTopic = "thingspeak"

    // fetch data from ThingSpeak
    def fetchData(): String = {
      val response = Http(apiUrl).asString
      response.body
    }

    // stream that fetches data every 15 seconds
    val dataStream = ssc.receiverStream(new Receiver[String](org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK_2) {
      override def onStart(): Unit = {
        while (!isStopped) {
          val data = fetchData()
          store(data)
          Thread.sleep(15000) // Sleep for 15 seconds
        }
      }

      override def onStop(): Unit = {}
    })


    dataStream.foreachRDD { rdd =>
      if (!rdd.isEmpty()) {
        val jsonData = rdd.collect().mkString("\n")
        if (jsonData.nonEmpty) {
          // Read JSON data into DataFrame
          val df = spark.read.json(Seq(jsonData).toDS())

          // Explode feeds array to flatten the data
          val explodedDF = df.withColumn("feeds", F.explode($"feeds"))

          // Select the desired fields from dataframe
          val resultDF = explodedDF.select(
            $"channel.name".as("channel_name"),
            $"feeds.created_at".as("timestamp"),
            $"feeds.field1".as("Wind Direction (North = 0 degrees)"),
            $"feeds.field2".as("Wind Speed (mph)"),
            $"feeds.field3".as("Humidity %"),
            $"feeds.field4".as("Temperature F"),
            $"feeds.field5".as("Rain (Inches/minute)"),
            $"feeds.field6".as("Pressure (\\\"Hg)"),
            $"feeds.field7".as("Power Level (V)"),
            $"feeds.field8".as("Light Intensity")
          ).withColumn("timestamp", F.to_timestamp($"timestamp"))

          // Send each row to Kafka
          resultDF.collect().foreach { row =>
            val key = row.getAs[String]("channel_name")
            val value = row.mkString(",") // Convert the row to a string
            producer.send(new ProducerRecord[String, String](kafkaTopic, key, value))
          }

          //val rowCount = resultDF.count()
         // println(s"Number of rows processed: $rowCount")

          //resultDF.show(truncate = false)
        }
      }
    }

    // Start the streaming context
    ssc.start()
    ssc.awaitTermination()

    // Close the producer after the streaming context is terminated
    producer.close() //
  }
}