package com.haiteam

import org.apache.spark.sql.SparkSession

object Example_03 {

  def main(args: Array[String]): Unit = {

    var spark = SparkSession.builder().appName("hkproject").
      config("spark.master", "local").
      getOrCreate()

    var staticUrl = "jdbc:mysql://192.168.110.112:3306/kopo"
    var staticUser = "root"
    var staticPw = "P@ssw0rd"
    var selloutDb = "KOPO_PRODUCT_VOLUME"

    val selloutDataFromMysql= spark.read.format("jdbc").
      options(Map("url" -> staticUrl,"dbtable" -> selloutDb,"user" -> staticUser, "password" -> staticPw)).load

    selloutDataFromMysql.createOrReplaceTempView("selloutTable")
    selloutDataFromMysql.show(3)

    var staticUrl1 = "jdbc:postgresql://192.168.110.111:5432/kopo"
    var staticUser1 = "kopo"
    var staticPw1 = "kopo"
    var selloutDb1 = "kopo_batch_season_mpara"

    val selloutDataFromMysql1= spark.read.format("jdbc").
      options(Map("url" -> staticUrl1,"dbtable" -> selloutDb1,"user" -> staticUser1, "password" -> staticPw1)).load

    selloutDataFromMysql1.createOrReplaceTempView("selloutTable")
    selloutDataFromMysql1.show(3)
  }
}
