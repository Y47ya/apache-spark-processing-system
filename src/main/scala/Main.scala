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

  val url = getProperties("url")
  val user = getProperties("user")
  val password = getProperties("password")

  private def getSchema: StructType = {
    new StructType()
      .add("dr_no", StringType)
      .add("date_rptd", StringType)
      .add("date_occ", StringType)
      .add("time_occ", StringType)
      .add("area", StringType)
      .add("area_name", StringType)
      .add("rpt_dist_no", StringType)
      .add("part_1_2", StringType)
      .add("crm_cd", StringType)
      .add("crm_cd_desc", StringType)
      .add("mocodes", StringType)
      .add("victid", StringType)
      .add("premis_cd", StringType)
      .add("premis_desc", StringType)
      .add("weapon_used_cd", StringType)
      .add("weapon_desc", StringType)
      .add("status", StringType)
      .add("status_desc", StringType)
      .add("crm_cd_1", StringType)
      .add("crm_cd_2", StringType)
      .add("crm_cd_3", StringType)
      .add("crm_cd_4", StringType)
      .add("location", StringType)
      .add("cross_street", StringType)
      .add("lat", StringType)
      .add("lon", StringType)
  }

  private def getSpark: SparkSession = {
    SparkSession
      .builder()
      .appName("CrimeETLStreaming")
      .master("local[*]")
      .getOrCreate()
  }

  private def getReadStream(spark: SparkSession): DataStreamReader = {
    spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", "1212")
  }

  private def cleanData(parsedDf: DataFrame, schema: StructType): DataFrame = {
    println("Cleaning batch data...")
    var dfProcessed = parsedDf

    val intCols = Seq(
      "dr_no", "time_occ", "area", "rpt_dist_no", "part_1_2",
      "crm_cd", "victid", "premis_cd", "weapon_used_cd",
      "crm_cd_1", "crm_cd_2", "crm_cd_3", "crm_cd_4"
    )
    val doubleCols = Seq("lat", "lon")

    intCols.foreach { colName =>
      dfProcessed = dfProcessed.withColumn(colName, coalesce(col(colName).cast(LongType), lit(0L)))
    }
    doubleCols.foreach { colName =>
      dfProcessed = dfProcessed.withColumn(colName, coalesce(col(colName).cast(DoubleType), lit(0.0)))
    }

    dfProcessed = dfProcessed
      .withColumn("date_rptd", to_date(col("date_rptd"), "MM/dd/yyyy hh:mm:ss a"))
      .withColumn("date_occ", to_date(col("date_occ"), "MM/dd/yyyy hh:mm:ss a"))
      .withColumn("mocodes", coalesce(col("mocodes"), lit("none")))
      .withColumn("weapon_desc", coalesce(col("weapon_desc"), lit("none")))
      .withColumn("cross_street", coalesce(col("cross_street"), lit("none")))

    val dateCols = Seq("date_rptd", "date_occ")
    val stringCols = schema.fields
      .filter(f => !intCols.contains(f.name) && !doubleCols.contains(f.name) && !dateCols.contains(f.name))
      .map(_.name)

    stringCols.foreach { colName =>
      dfProcessed = dfProcessed.withColumn(
        colName,
        regexp_replace(trim(regexp_replace(col(colName), "\u00A0", " ")), "\\s+", " ")
      )
    }

    println("Data cleaning complete.")
    dfProcessed
  }

  private def transformData(df: DataFrame): Map[String, DataFrame] = {
    println("Transforming data into separate tables...")

    val areasTable = df.select(col("area").alias("areaid"), col("area_name").alias("areaname")).dropDuplicates()
    val statusTable = df.select(col("status").alias("statusid"), col("status_desc").alias("statusdesc")).dropDuplicates()
    val weaponsTable = df.select(col("weapon_used_cd").alias("weaponid"), col("weapon_desc").alias("weapondesc")).dropDuplicates()

    val crimesLong = df.select(col("dr_no"),
        expr("stack(4, crm_cd_1, crm_cd_2, crm_cd_3, crm_cd_4) as CrmCd"))
      .filter(col("CrmCd").isNotNull && col("CrmCd") =!= 0L)

    val crimesTable = df.select(col("crm_cd").alias("crmcd"), col("crm_cd_desc").alias("crmcddesc"), col("part_1_2").alias("part1_2")).dropDuplicates()
    val crimeReportCrimes = crimesLong.select(col("dr_no"), col("CrmCd").alias("crmcd")).dropDuplicates()

    val crimeReports = df.select(
      col("dr_no"),
      col("victid"),
      col("date_rptd").alias("daterptd"),
      col("date_occ").alias("dateocc"),
      col("time_occ").alias("timeocc"),
      col("area").alias("areaid"),
      col("rpt_dist_no").alias("rptdistno"),
      col("premis_cd").alias("premisid"),
      col("premis_desc").alias("premisdesc"),
      col("location"),
      col("cross_street").alias("crossstreet"),
      col("lat"),
      col("lon"),
      col("status").alias("statusid"),
      col("mocodes"),
      col("weapon_used_cd").alias("weaponid")
    ).dropDuplicates()

    println("Data transformation complete.")
    Map(
      "areas"             -> areasTable,
      "status"            -> statusTable,
      "crimes"            -> crimesTable,
      "weapons"           -> weaponsTable,
      "crimereports"      -> crimeReports,
      "crimereportcrimes" -> crimeReportCrimes
    )
  }

  private def getProperties: Map[String, String] = {
    Map(
      "url"      -> "jdbc:postgresql://localhost:5432/spark_crime_db",
      "user"     -> "postgres",
      "password" -> "postgres"
    )
  }

  private def setProperty(user: String, password: String): Properties = {
    val pgProperties = new Properties()
    pgProperties.setProperty("user", getProperties("user"))
    pgProperties.setProperty("password", getProperties("password"))
    pgProperties.setProperty("driver", "org.postgresql.Driver")
    pgProperties
  }

  // ── helper: read a single column from the DB ─────────────────────────────
  private def readFromDB(spark: SparkSession, table: String, column: String): DataFrame = {
    spark.read
      .format("jdbc")
      .option("url", url)
      .option("dbtable", table)
      .option("user", user)
      .option("password", password)
      .load()
      .select(column)
  }

  // ── helper: insert missing keys via ON CONFLICT DO NOTHING ───────────────
  private def insertMissing(sql: String): Unit = {
    val conn = DriverManager.getConnection(url, user, password)
    conn.setAutoCommit(false)
    val stmt = conn.createStatement()
    stmt.execute(sql)
    stmt.execute("COMMIT;")
    conn.commit()
    stmt.close()
    conn.close()
  }

  private def writeTable(df: DataFrame, tableName: String, primaryKey: String): Unit = {
    println(s"Starting insertion for table: $tableName")

    val pgProperties = setProperty(user, password)
    val tempTable = s"${tableName}_staging"

    // ✅ Overwrite so staging table is always recreated with the current schema
    df.write.mode(SaveMode.Overwrite).jdbc(url, tempTable, pgProperties)
    println(s"Data written to staging table: $tempTable")

    val conn = DriverManager.getConnection(url, user, password)
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

  private def ensureDefaultFKValues(spark: SparkSession): Unit = {
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

  // ── FK checks — all read from DB, all use ON CONFLICT DO NOTHING ─────────

  private def checkAreasFK(childDF: DataFrame, fkColumn: String, spark: SparkSession): Unit = {
    val parentKeys = readFromDB(spark, "areas", "areaid")
    val missingKeys = childDF.select(fkColumn).distinct().except(parentKeys)
    if (missingKeys.count() > 0) {
      missingKeys.collect().foreach { row =>
        insertMissing(s"INSERT INTO areas (areaid, areaname) VALUES (${row.getLong(0)}, 'unknown') ON CONFLICT (areaid) DO NOTHING;")
      }
    }
  }

  private def checkStatusFK(childDF: DataFrame, fkColumn: String, spark: SparkSession): Unit = {
    val parentKeys = readFromDB(spark, "status", "statusid")
    val missingKeys = childDF.select(fkColumn).distinct().except(parentKeys)
    if (missingKeys.count() > 0) {
      missingKeys.collect().foreach { row =>
        insertMissing(s"INSERT INTO status (statusid, statusdesc) VALUES ('${row.getString(0)}', 'unknown') ON CONFLICT (statusid) DO NOTHING;")
      }
    }
  }

  private def checkWeaponsFK(childDF: DataFrame, fkColumn: String, spark: SparkSession): Unit = {
    val parentKeys = readFromDB(spark, "weapons", "weaponid")
    val missingKeys = childDF.select(fkColumn).distinct().except(parentKeys)
    if (missingKeys.count() > 0) {
      missingKeys.collect().foreach { row =>
        insertMissing(s"INSERT INTO weapons (weaponid, weapondesc) VALUES (${row.getLong(0)}, 'unknown') ON CONFLICT (weaponid) DO NOTHING;")
      }
    }
  }

  private def checkCrimesFK(childDF: DataFrame, fkColumn: String, spark: SparkSession): Unit = {
    val parentKeys = readFromDB(spark, "crimes", "crmcd")
    val missingKeys = childDF.select(fkColumn).distinct().except(parentKeys)
    if (missingKeys.count() > 0) {
      missingKeys.collect().foreach { row =>
        insertMissing(s"INSERT INTO crimes (crmcd, crmcddesc, part1_2) VALUES (${row.getLong(0)}, 'unknown', 'unknown') ON CONFLICT (crmcd) DO NOTHING;")
      }
    }
  }

  private def checkCrimeReportsFK(childDF: DataFrame, fkColumn: String, spark: SparkSession): Unit = {
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

  private def ingestDataWithFKChecks(tables: Map[String, DataFrame], spark: SparkSession): Unit = {
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

  def main(args: Array[String]): Unit = {
    val schema = getSchema
    val spark  = getSpark
    spark.sparkContext.setLogLevel("ERROR")

    val data     = getReadStream(spark).load()
    val parsedDf = data.select(from_json(col("value"), schema).alias("data")).select("data.*")

    parsedDf.writeStream
      .foreachBatch { (batchDf: DataFrame, batchId: Long) =>
        val df     = cleanData(batchDf, schema)
        val tables = transformData(df)
        ensureDefaultFKValues(spark)
        ingestDataWithFKChecks(tables, spark)
      }
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .start()
      .awaitTermination()
  }
}