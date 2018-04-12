package com.haiteam

import org.apache.spark.sql.SparkSession

object Example_Seasonality {
  def main(args: Array[String]): Unit = {
    var spark = SparkSession.builder().appName("hkproject").
      config("spark.master", "local").
      getOrCreate()

    // 변수를 생성하고 임시 테이블을 생성하세요.
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
    productMasterDf.createOrReplaceTempView("selloutTable1")
  }

}
