
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

case class WeatherData(station: String,
                       timestamp: String,
                       Wind_Direction: Double,
                       Wind_Speed: Double,
                       Humidity: Double,
                       Temperature: Double,
                       Rain: Double,
                       Pressure: Double,
                       Power_Level: Double
                       , Light_Intensity: Double)
object KafkaConsumer {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("NetworkWordCount")
    val ssc = new StreamingContext(conf, Seconds(15))

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val topics = Array("thingspeak-data")
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    stream.foreachRDD { rdd =>
      rdd.foreach { record =>
        val data = record.value().split(",")
        if (data.length == 10) {
          val weatherData = WeatherData(
            station = data(0),
            timestamp = data(1),
            Wind_Direction = data(2).toDouble,
            Wind_Speed = data(3).toDouble,
            Humidity = data(4).toDouble,
            Temperature = data(5).toDouble,
            Rain = data(6).toDouble,
            Pressure = data(7).toDouble,
            Power_Level = data(8).toDouble,
            Light_Intensity = data(9).toDouble
          )
          Thread.sleep(15000) // Sleep for 15 seconds
          // Process the weatherData object as needed
          println(weatherData)
        }
      }
    }
    // Await termination of the query
    ssc.start()
    ssc.awaitTermination()

  }
}