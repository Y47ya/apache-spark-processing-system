package etl

import org.apache.spark.sql.SparkSession


object SparkSessionManager {

  val url = getProperties("url")
  val user = getProperties("user")
  val password = getProperties("password")

  private def getProperties: Map[String, String] = {
    Map(
      "url"      -> "jdbc:postgresql://localhost:5432/spark_crime_db",
      "user"     -> "postgres",
      "password" -> "postgres"
    )
  }

  private[etl] def getSpark: SparkSession = {
    SparkSession
      .builder()
      .appName("CrimeETLStreaming")
      .master("local[*]")
      .getOrCreate()
  }

}
