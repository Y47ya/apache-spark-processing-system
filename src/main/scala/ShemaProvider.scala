package etl

import org.apache.spark.sql.types.{StringType, StructType}


object ShemaProvider {

  private[etl] def getSchema: StructType = {
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

}
