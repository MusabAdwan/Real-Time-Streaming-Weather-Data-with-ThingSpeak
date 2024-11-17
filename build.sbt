
name := "webserver"

version := "1.0"

scalaVersion := "2.12.15"  // Make sure this matches Spark version compatibility

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-http" % "10.2.6",
  "com.typesafe.akka" %% "akka-stream" % "2.6.18",
  "com.typesafe.akka" %% "akka-http-spray-json" % "10.2.6",
"org.apache.spark" %% "spark-core" % "3.3.2",
"org.apache.spark" %% "spark-sql" % "3.3.2",
"org.scalaj" %% "scalaj-http" % "2.4.2",
"org.apache.spark" %% "spark-streaming" % "3.3.2",
"org.mongodb.spark" %% "mongo-spark-connector" % "3.0.1",
"org.apache.spark" %% "spark-streaming-kafka-0-10" % "3.4.0",
"org.apache.kafka" % "kafka-clients" % "3.4.0" ,
"com.squareup.okhttp3" % "okhttp" % "4.9.2",
//"org.apache.kafka" %% "kafka" % "3.0.0",
"com.typesafe.akka" %% "akka-stream" % "2.6.18",
"com.typesafe.akka" %% "akka-http" % "10.2.6",
"com.typesafe.akka" %% "akka-actor-typed" % "2.6.18"
)