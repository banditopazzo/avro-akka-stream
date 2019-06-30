name := "avro-akka-stream"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies ++= Seq(
  "org.apache.hadoop" % "hadoop-common" % "3.1.1",
  "com.sksamuel.avro4s" %% "avro4s-core" % "2.0.2",
  "com.typesafe.akka" %% "akka-stream" % "2.5.23"
)