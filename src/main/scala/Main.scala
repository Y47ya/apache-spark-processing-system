package etl


import org.apache.spark.sql.SparkSession
import org.apache.spark._
import org.apache.spark.sql.types.StructType
import org.apache.spark.streaming._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{DataStreamReader, Trigger}
import org.apache.spark.sql.{DataFrame, SaveMode}
import java.util.Properties
import java.sql.DriverManager


object Main {





  def main(args: Array[String]): Unit = {
    val schema = ShemaProvider.getSchema
    val spark  = SparkSessionManager.getSpark
    spark.sparkContext.setLogLevel("ERROR")

    val data     = DBHelper.getReadStream(spark).load()
    val parsedDf = data.select(from_json(col("value"), schema).alias("data")).select("data.*")

    parsedDf.writeStream
      .foreachBatch { (batchDf: DataFrame, batchId: Long) =>
        val df     = DataCleaner.cleanData(batchDf, schema)
        val tables = DataCleaner.transformData(df)
        DBHelper.ensureDefaultFKValues(spark)
        DBHelper.ingestDataWithFKChecks(tables, spark)
      }
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .start()
      .awaitTermination()
  }
}