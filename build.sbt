name := "ProyectoScalaSpark243"

version := "0.1"

scalaVersion := "2.12.8"

val sparkVersion = "2.4.3"
val sparkStreamingTwitterVersion = "1.6.3"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-hive" % sparkVersion,
  "mysql" % "mysql-connector-java" % "8.0.16"
)

mainClass in (Compile, run) := Some("word_count.WordCountNew")
