import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.Encoders
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.log4j.BasicConfigurator
import org.apache.log4j.varia.NullAppender
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{MinMaxScaler, StandardScaler, StringIndexer, VectorAssembler}
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.evaluation.{BinaryClassificationEvaluator, MulticlassClassificationEvaluator}
import org.apache.spark.sql.functions._
import org.apache.spark.ml.tuning.{CrossValidator, CrossValidatorModel, ParamGridBuilder}
import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import org.apache.log4j.BasicConfigurator
import org.apache.log4j.varia.NullAppender
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.ml.tuning.CrossValidatorModel
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types.DoubleType
import java.io.File
case class WeatherData(station: String,
                       timestamp: String,
                       Wind_Direction: Int,
                       Wind_Speed: Double,
                       Humidity: Int,
                       Temperature: Double,
                       Rain: Double,
                       Pressure: Double,
                       Power_Level: Double,
                       Light_Intensity: Int)
object KafkaConsumer {

  def main(args: Array[String]): Unit = {
    val nullAppender = new NullAppender
    BasicConfigurator.configure(nullAppender)
    // Initialize Spark session and configuration

    val spark = SparkSession.builder()
      .appName("NetworkWordCount")
      .master("local[*]") // Run Spark locally using all available cores
      // .config("spark.executor.memory", "4g")  // Set executor memory
      //.config("spark.driver.memory", "4g")    // Set driver memory
      .getOrCreate()

    import spark.implicits._

    // Load the saved model
    val modelPath = "models/WeatherActivityModel"
    val model = CrossValidatorModel.load(modelPath)

    val ssc = new StreamingContext(spark.sparkContext, Seconds(15))

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
      // Filter and map records to WeatherData
      val weatherRecords = rdd.collect {
        case record if record.value().split(",").length == 10 =>
          val data = record.value().split(",")
          WeatherData(
            station = data(0),
            timestamp = data(1),
            Wind_Direction = data(2).toInt,
            Wind_Speed = data(3).toDouble,
            Humidity = data(4).toInt,
            Temperature = data(5).toDouble,
            Rain = data(6).toDouble,
            Pressure = data(7).toDouble,
            Power_Level = data(8).toDouble,
            Light_Intensity = data(9).toInt
          )
      }
      println(weatherRecords)
      val weatherDF = spark.createDataset(weatherRecords)(Encoders.product[WeatherData])
        // Print sample data for debugging

        weatherDF.show()
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
          predictions.show()
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
        }
    // Thread.sleep(15000) // Sleep for 15 seconds
    // Await termination of the query
    ssc.start()
    ssc.awaitTermination()

  }
}