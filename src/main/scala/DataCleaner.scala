package etl

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{coalesce, col, date_format, expr, lit, month, regexp_replace, to_date, trim, when, year}
import org.apache.spark.sql.types.{DoubleType, LongType, StructType}

object DataCleaner {

  private[etl] def cleanData(parsedDf: DataFrame, schema: StructType): DataFrame = {
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

  private[etl] def transformData(df: DataFrame): Map[String, DataFrame] = {
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

    val enrichedCrimeReports = crimeReports
      .withColumn("year",     year(col("dateocc")))
      .withColumn("month",    month(col("dateocc")))
      .withColumn("weekday",  date_format(col("dateocc"), "EEEE"))
      .withColumn("dayperiod",
        when(col("timeocc") < "0600", "night")
          .when(col("timeocc") < "1200", "morning")
          .when(col("timeocc") < "1800", "afternoon")
          .otherwise("evening")
      )
      .withColumn("isweekend",
        when(col("weekday").isin("Saturday", "Sunday"), lit(true)).otherwise(lit(false))
      )
      .withColumn("hasweapon",
        when(col("weaponid").isNull || col("weaponid") === 0L, lit(false)).otherwise(lit(true))
      )

    println("Data transformation complete.")
    Map(
      "areas"             -> areasTable,
      "status"            -> statusTable,
      "crimes"            -> crimesTable,
      "weapons"           -> weaponsTable,
      "crimereports"      -> enrichedCrimeReports,
      "crimereportcrimes" -> crimeReportCrimes
    )
  }



}
