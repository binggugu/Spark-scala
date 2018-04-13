package com.haiteam

import org.apache.spark.sql.SparkSession

object Example_Seasonality {
  def main(args: Array[String]): Unit = {
    var spark = SparkSession.builder().appName("hkproject").
      config("spark.master", "local").
      getOrCreate()

    // 변수를 생성하고 임시 테이블을 생성하세요.
    // oracle connection
    var staticUrl = "jdbc:oracle:thin:@192.168.110.111:1521/orcl"
    var staticUser = "kopo"
    var staticPw = "kopo"
    var selloutDb = "kopo_channel_seasonality_new"
    var productNameDb = "kopo_product_mst"

    val selloutData= spark.read.format("jdbc").
      options(Map("url" -> staticUrl,"dbtable" -> selloutDb,"user" -> staticUser, "password" -> staticPw)).load

    val productMasterDf = spark.read.format("jdbc").
      options(Map("url" -> staticUrl,"dbtable" -> productNameDb, "user" -> staticUser, "password" -> staticPw)).load

    selloutData.createOrReplaceTempView("selloutTable")
    productMasterDf.createOrReplaceTempView("mstTable")

    var middleResult = spark.sql("select a.regionid," +
      "concat(a.regionid,'_',a.product) as keycol, " +
      "a.redionid as accountid, " +
      "a.product, " +
      "a.yearweek, " +
      "cast(a.qty as double) as qty, " +
      "b.product_name " +
      "from selloutTable a " +
      "left join mstTable b " +
      "on a.product = b.product_id")

    var rawData = spark.sql("select concat(a.regionid,'_',a.product) as keycol, " +
      "a.regionid as accountid, " +
      "a.product, " +
      "a.yearweek, " +
      "cast(a.qty as String) as qty, " +
      "'test' as productname from keydata a" )

  }

}
