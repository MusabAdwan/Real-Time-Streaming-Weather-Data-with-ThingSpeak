

version := "1.0"

scalaVersion := "2.12.15" // Use the appropriate Scala version

libraryDependencies ++= {
  Seq(
    "org.apache.spark" %% "spark-core" % "3.3.2",
    "org.apache.spark" %% "spark-sql" % "3.3.2",
    "org.scalaj" %% "scalaj-http" % "2.4.2",
    "org.apache.spark" %% "spark-streaming" % "3.3.2",
    "org.mongodb.spark" %% "mongo-spark-connector" % "3.0.1",
    "org.apache.spark" %% "spark-streaming-kafka-0-10" % "3.1.1",
   // "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.3.2",
      "org.apache.kafka" % "kafka-clients" % "3.4.0" ,// Adjust Kafka version as needed
    "com.squareup.okhttp3" % "okhttp" % "4.9.2",
    //"org.apache.kafka" %% "kafka" % "3.1.1", // Kafka client for Scala
    "com.typesafe.akka" %% "akka-stream" % "2.6.18", // Akka Streams
    "com.typesafe.akka" %% "akka-http" % "10.2.6", // Akka HTTP for making API calls
    "com.typesafe.akka" %% "akka-actor-typed" % "2.6.18")
}
//libraryDependencies += "org.apache.spark" %% "spark-core" % "3.3.2" // Use the appropriate Spark version
lazy val root = (project in file("."))
  .settings(
    name := "Consumer"
  )
resolvers += "MongoDB Repository" at "https://repo.mongodb.org/maven2/"