import org.apache.log4j.BasicConfigurator
import org.apache.log4j.varia.NullAppender
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{MinMaxScaler, StandardScaler, StringIndexer, VectorAssembler}
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.evaluation.{BinaryClassificationEvaluator, MulticlassClassificationEvaluator}
import org.apache.spark.sql.functions._
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.ml.PipelineModel

import java.io.File



object WeatherBasedActivityRecommendation {
  def deleteFile(filePath: String): Boolean = {
    val file = new File(filePath)
    if (file.exists()) {
      try {
        // If it's a directory, delete all files and subdirectories inside it first
        if (file.isDirectory) {
          file.listFiles().foreach(file => deleteFile(file.getPath)) // Recursively delete files in the directory
        }
        // Now delete the directory or file
        file.delete()
        println(s"File or directory at $filePath deleted successfully.")
        true
      } catch {
        case e: Exception =>
          println(s"Failed to delete file at $filePath. Error: ${e.getMessage}")
          false
      }
    } else {
      println(s"File or directory at $filePath does not exist.")
      false
    }
  }
  def main(args: Array[String]): Unit = {
    // Initialize Spark Session
    val nullAppender = new NullAppender
    BasicConfigurator.configure(nullAppender)
    val spark = SparkSession.builder()
      .appName("Robust Activity Recommendation System")
      .master("local[*]")
      .config("spark.executor.memory", "4g")  // Set executor memory
      .config("spark.driver.memory", "4g")    // Set driver memory
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    // Load dataset from CSV
    val dataPath = "data/thingspeak_data100min.csv"
    var weatherData = spark.read.option("header", "true").option("inferSchema", "true").csv(dataPath)

    // Handle missing values by filling with median for numerical columns (more robust than dropping rows)
    val numericColumns = weatherData.columns.filter(c => weatherData.schema(c).dataType == "DoubleType" || weatherData.schema(c).dataType == "IntegerType")

    // Fill missing values with the median of each column (for robustness)
    numericColumns.foreach { col =>
      val median = weatherData.stat.approxQuantile(col, Array(0.5), 0.0)(0)
      weatherData = weatherData.na.fill(Map(col -> median))
    }

    // Add activity preference based on weather conditions, with more realistic thresholds
    /*weatherData = weatherData.withColumn("activity_preference",
      when(col("Temperature (F)") > 75 && col("Rain (Inches/minute)") == 0, "Running")
        .when(col("Wind Speed (mph)") > 15 && col("Rain (Inches/minute)") == 0 && col("Temperature (F)") <= 50, "Walking")
        .when(col("Rain (Inches/minute)") > 0.2, "Stay Home")
        .when(col("Temperature (F)") <= 50, "Stay Home")
        .otherwise("you are free")
    )*/


    weatherData = weatherData.withColumn("activity_preference",
      when(col("Temperature (F)") > 75 && col("Rain (Inches/minute)") <= 0.4 && col("Wind Speed (mph)") < 15 && col("Light Intensity") > 500, "Running")
        .when(col("Temperature (F)") <= 75 && col("Temperature (F)") >= 60 && col("Wind Speed (mph)") < 10 && col("Rain (Inches/minute)") === 0 && col("Light Intensity") > 300, "Picnicking")
        .when(col("Wind Speed (mph)") > 15 && col("Rain (Inches/minute)")<= 0.4 && col("Temperature (F)") <= 50 && col("Light Intensity") > 200, "Walking")
        .when(col("Rain (Inches/minute)") > 0.5, "Stay Home")
        .when(col("Temperature (F)") <= 45 || col("Wind Speed (mph)") > 25 || col("Light Intensity") < 100, "Stay Home")
        .when(col("Temperature (F)") >= 60 && col("Temperature (F)") <= 85 && col("Rain (Inches/minute)") <= 0.4 && col("Wind Speed (mph)") < 10 && col("Light Intensity") > 500, "Reading Outdoors")
        .when(col("Temperature (F)") >= 65 && col("Temperature (F)") <= 80 && col("Rain (Inches/minute)") <= 0.4 && col("Wind Speed (mph)") < 10 && col("Light Intensity") > 300, "Barbecue")
        
        .when(col("Temperature (F)") >= 70 && col("Temperature (F)") <= 85 && col("Wind Speed (mph)") < 5 && col("Rain (Inches/minute)") === 0 && col("Light Intensity") > 500, "Tennis")
        .when(col("Temperature (F)") >= 60 && col("Temperature (F)") <= 75 && col("Rain (Inches/minute)") <= 0.4 && col("Wind Speed (mph)") < 5 && col("Light Intensity") > 200, "Yoga Outdoors")
        .otherwise("you are free")
    )



    // Preprocess data: Convert categorical labels to numeric using StringIndexer
    val labelIndexer = new StringIndexer()
      .setInputCol("activity_preference")
      .setOutputCol("label")

    // Assemble features from relevant columns
    val assembler = new VectorAssembler()
      .setInputCols(Array("Wind Direction ", "Wind Speed (mph)", "% Humidity",
        "Temperature (F)", "Rain (Inches/minute)", "Pressure (Hg)", "Light Intensity"))
      .setOutputCol("features")

    // Normalize the features using MinMaxScaler
    val scaler = new MinMaxScaler()
      .setInputCol("features")
      .setOutputCol("scaledFeatures")

    // Define a RandomForestClassifier (with tuned parameters later)
    val classifier = new RandomForestClassifier()
      .setLabelCol("label")
      .setFeaturesCol("scaledFeatures")
      .setNumTrees(100)  // Increased number of trees for better model performance

    // Build a pipeline: Combine all stages (indexing, assembling, scaling, and classification)
    val pipeline = new Pipeline()
      .setStages(Array(labelIndexer, assembler, scaler, classifier))

    // Split the data into training and test sets
    val Array(trainingData, testData) = weatherData.randomSplit(Array(0.8, 0.2))

    // Hyperparameter tuning using Cross-Validation
    val paramGrid = new ParamGridBuilder()
      .addGrid(classifier.numTrees, Array(50, 100))
      .addGrid(classifier.maxDepth, Array(5, 10, 15))
      .build()

    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")

    val crossValidator = new CrossValidator()
      .setEstimator(pipeline)
      .setEvaluator(evaluator)
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(3)  // 3-fold cross-validation

    // Train the model with cross-validation
    val cvModel = crossValidator.fit(trainingData)

    // Evaluate the model
    val predictions = cvModel.transform(testData)
    predictions.select("features", "label", "prediction").show(10)

    // Map numeric predictions back to activity names
    val activityMap = Map(
      0.0 -> "Stay Home",       // Example: 0 -> Stay Home
      1.0 -> "Walking",         // Example: 1 -> Walking
      2.0 -> "Running",         // Example: 2 -> Running
      3.0 -> "Picnicking",      // Example: 3 -> Picnicking
      4.0 -> "Reading Outdoors",// Example: 4 -> Reading Outdoors
      5.0 -> "Barbecue",        // Example: 5 -> Barbecue
     
      6.0 -> "Tennis",          // Example: 6 -> Tennis
      7.0 -> "Yoga Outdoors",   // Example: 7 -> Yoga Outdoors
      8.0 -> "you are free"     // Example: 8 -> No specific recommendation, free to choose
    )

    predictions.show(10,truncate = false)
    // Apply the activity mapping to the predictions
    val predictedActivities = predictions.withColumn("recommended_activity",
      udf((prediction: Double) => activityMap.getOrElse(prediction, "Stay Home")).apply(col("prediction")))

    // Select relevant columns for the final output: Including original weather data and the recommended activity
    val finalOutput = predictedActivities
      .select("Wind Direction ", "Wind Speed (mph)", "% Humidity", "Temperature (F)", "Rain (Inches/minute)",
        "Pressure (Hg)", "Light Intensity", "recommended_activity")
    finalOutput.show(10 , truncate = false)
    finalOutput.groupBy(col("recommended_activity")).count().show()
    // Save the predictions to a CSV file
    val outputPath = "output/robust_weather_activity_recommendations1.csv"

    deleteFile(outputPath)
    finalOutput.write
      .option("header", "true")
      .csv(outputPath)

    println(s"Predictions saved to: $outputPath")
    // Save the trained model to disk
    val modelPath = "models/WeatherActivityModel"
    cvModel.write.overwrite().save(modelPath)

    // Evaluation Metrics
    val accuracy = evaluator.evaluate(predictions)
    println(s"Test Accuracy = ${(accuracy * 100).formatted("%.2f")}%")

    // F1 Score, Precision, Recall Calculation using BinaryClassificationEvaluator
    val f1Evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("f1")

    val precisionEvaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("weightedPrecision")

    val recallEvaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("weightedRecall")

    val f1Score = f1Evaluator.evaluate(predictions)
    val precision = precisionEvaluator.evaluate(predictions)
    val recall = recallEvaluator.evaluate(predictions)

    println(s"F1 Score = ${(f1Score * 100).formatted("%.2f")}%")
    println(s"Precision = ${(precision * 100).formatted("%.2f")}%")
    println(s"Recall = ${(recall * 100).formatted("%.2f")}%")

    // Stop Spark session
    spark.stop()
  }
}
