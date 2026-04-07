package etl

//import etl.Main.{getProperties, insertMissing, readFromDB, setProperty, writeTable}
import org.apache.spark.sql.functions.user
import org.apache.spark.sql.streaming.DataStreamReader
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.sql.DriverManager
import java.util.Properties


object DBHelper {

  private[etl] def getReadStream(spark: SparkSession): DataStreamReader = {
    spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", "1212")
  }

  private def readFromDB(spark: SparkSession, table: String, column: String): DataFrame = {
    spark.read
      .format("jdbc")
      .option("url", SparkSessionManager.url)
      .option("dbtable", table)
      .option("user", SparkSessionManager.user)
      .option("password", SparkSessionManager.password)
      .load()
      .select(column)
  }

  private def insertMissing(sql: String): Unit = {
    val conn = DriverManager.getConnection(SparkSessionManager.url, SparkSessionManager.user, SparkSessionManager.password)
    conn.setAutoCommit(false)
    val stmt = conn.createStatement()
    stmt.execute(sql)
    stmt.execute("COMMIT;")
    conn.commit()
    stmt.close()
    conn.close()
  }

  private[etl] def checkAreasFK(childDF: DataFrame, fkColumn: String, spark: SparkSession): Unit = {
    val parentKeys = readFromDB(spark, "areas", "areaid")
    val missingKeys = childDF.select(fkColumn).distinct().except(parentKeys)
    if (missingKeys.count() > 0) {
      missingKeys.collect().foreach { row =>
        insertMissing(s"INSERT INTO areas (areaid, areaname) VALUES (${row.getLong(0)}, 'unknown') ON CONFLICT (areaid) DO NOTHING;")
      }
    }
  }

  private[etl] def checkStatusFK(childDF: DataFrame, fkColumn: String, spark: SparkSession): Unit = {
    val parentKeys = readFromDB(spark, "status", "statusid")
    val missingKeys = childDF.select(fkColumn).distinct().except(parentKeys)
    if (missingKeys.count() > 0) {
      missingKeys.collect().foreach { row =>
        insertMissing(s"INSERT INTO status (statusid, statusdesc) VALUES ('${row.getString(0)}', 'unknown') ON CONFLICT (statusid) DO NOTHING;")
      }
    }
  }

  private[etl] def checkWeaponsFK(childDF: DataFrame, fkColumn: String, spark: SparkSession): Unit = {
    val parentKeys = readFromDB(spark, "weapons", "weaponid")
    val missingKeys = childDF.select(fkColumn).distinct().except(parentKeys)
    if (missingKeys.count() > 0) {
      missingKeys.collect().foreach { row =>
        insertMissing(s"INSERT INTO weapons (weaponid, weapondesc) VALUES (${row.getLong(0)}, 'unknown') ON CONFLICT (weaponid) DO NOTHING;")
      }
    }
  }

  private[etl] def checkCrimesFK(childDF: DataFrame, fkColumn: String, spark: SparkSession): Unit = {
    val parentKeys = readFromDB(spark, "crimes", "crmcd")
    val missingKeys = childDF.select(fkColumn).distinct().except(parentKeys)
    if (missingKeys.count() > 0) {
      missingKeys.collect().foreach { row =>
        insertMissing(s"INSERT INTO crimes (crmcd, crmcddesc, part1_2) VALUES (${row.getLong(0)}, 'unknown', 'unknown') ON CONFLICT (crmcd) DO NOTHING;")
      }
    }
  }

  private[etl] def checkCrimeReportsFK(childDF: DataFrame, fkColumn: String, spark: SparkSession): Unit = {
    val parentKeys = readFromDB(spark, "crimereports", "dr_no")
    val missingKeys = childDF.select(fkColumn).distinct().except(parentKeys)
    if (missingKeys.count() > 0) {
      missingKeys.collect().foreach { row =>
        insertMissing(
          s"""INSERT INTO crimereports (dr_no, victid, daterptd, dateocc, timeocc, areaid, rptdistno, premisid, premisdesc, location, crossstreet, lat, lon, statusid, mocodes, weaponid)
             |VALUES (${row.getLong(0)}, 0, NULL, NULL, 0, 0, 0, 0, 'unknown', 'unknown', 'none', 0.0, 0.0, 'unknown', 'none', 0)
             |ON CONFLICT (dr_no) DO NOTHING;
           """.stripMargin
        )
      }
    }
  }

  private[etl] def ensureDefaultFKValues(spark: SparkSession): Unit = {
    val defaults = Seq(
      ("areas",   "areaid",   "0"),
      ("weapons", "weaponid", "0"),
      ("status",  "statusid", "'unknown'"),
      ("crimes",  "crmcd",    "0")
    )
    defaults.foreach { case (table, column, value) =>
      insertMissing(
        s"""INSERT INTO $table ($column)
           |SELECT $value
           |WHERE NOT EXISTS (SELECT 1 FROM $table WHERE $column = $value)
         """.stripMargin
      )
    }
  }

  private def setProperty(user: String, password: String): Properties = {
    val pgProperties = new Properties()
    pgProperties.setProperty("user", SparkSessionManager.user)
    pgProperties.setProperty("password", SparkSessionManager.password)
    pgProperties.setProperty("driver", "org.postgresql.Driver")
    pgProperties
  }

  private def writeTable(df: DataFrame, tableName: String, primaryKey: String): Unit = {
    println(s"Starting insertion for table: $tableName")

    val pgProperties = setProperty(SparkSessionManager.user, SparkSessionManager.password)
    val tempTable = s"${tableName}_staging"

    df.write.mode(SaveMode.Overwrite).jdbc(SparkSessionManager.url, tempTable, pgProperties)
    println(s"Data written to staging table: $tempTable")

    val conn = DriverManager.getConnection(SparkSessionManager.url, SparkSessionManager.user, SparkSessionManager.password)
    conn.setAutoCommit(false)
    val stmt = conn.createStatement()
    val columns = df.columns

    val setClause = primaryKey match {
      case pk if pk.contains(",") =>
        val pkColumns = pk.split(",").map(_.trim)
        columns.filterNot(pkColumns.contains).map(c => s"$c = EXCLUDED.$c").mkString(", ")
      case pk =>
        columns.filter(_ != pk).map(c => s"$c = EXCLUDED.$c").mkString(", ")
    }

    val query = tableName match {
      case "crimereportcrimes" =>
        s"INSERT INTO $tableName (${columns.mkString(", ")}) SELECT ${columns.mkString(", ")} FROM $tempTable ON CONFLICT ($primaryKey) DO NOTHING;"
      case _ =>
        s"INSERT INTO $tableName (${columns.mkString(", ")}) SELECT ${columns.mkString(", ")} FROM $tempTable ON CONFLICT ($primaryKey) DO UPDATE SET $setClause;"
    }

    stmt.execute(query)
    stmt.execute("COMMIT;")
    println(s"Table $tableName inserted into main table.")
    stmt.execute(s"DROP TABLE IF EXISTS $tempTable;")
    println(s"Staging table $tempTable dropped.")

    conn.commit()
    stmt.close()
    conn.close()
    println(s"Insertion for table $tableName completed.\n")
  }


  private[etl] def ingestDataWithFKChecks(tables: Map[String, DataFrame], spark: SparkSession): Unit = {
    println("Starting FK checks...")

    checkAreasFK(tables("crimereports"),      "areaid",  spark)
    checkStatusFK(tables("crimereports"),     "statusid", spark)
    checkWeaponsFK(tables("crimereports"),    "weaponid", spark)
    checkCrimesFK(tables("crimereportcrimes"), "crmcd",   spark)
    checkCrimeReportsFK(tables("crimereportcrimes"), "dr_no", spark)

    println("FK checks completed.")

    if (tables("areas").count() > 0)             writeTable(tables("areas"),             "areas",             "areaid")
    if (tables("status").count() > 0)            writeTable(tables("status"),            "status",            "statusid")
    if (tables("crimes").count() > 0)            writeTable(tables("crimes"),            "crimes",            "crmcd")
    if (tables("weapons").count() > 0)           writeTable(tables("weapons"),           "weapons",           "weaponid")
    if (tables("crimereports").count() > 0)      writeTable(tables("crimereports"),      "crimereports",      "dr_no")
    if (tables("crimereportcrimes").count() > 0) writeTable(tables("crimereportcrimes"), "crimereportcrimes", "dr_no, crmcd")
  }



}
