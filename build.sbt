ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.18"

lazy val root = (project in file("."))
  .settings(
    name := "project",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "3.5.0",
      "org.apache.spark" %% "spark-sql" % "3.5.0",
      "org.apache.spark" %% "spark-streaming" % "3.5.0",
      "org.apache.spark" %% "spark-streaming-kafka-0-10" % "3.5.0",
      "org.apache.parquet" % "parquet-avro" % "1.12.3",
      "org.postgresql" % "postgresql" % "42.7.8"
    )
  )
