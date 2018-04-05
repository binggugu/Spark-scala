package com.haiteam

import  org.apache.spark.sql.SparkSession;

object Example_Join {
  def main(args: Array[String]): Unit = {

  var spark = SparkSession.builder().appName("hkproject").
    config("spark.master", "local").
    getOrCreate()

  var dataPath = "c:/spark/bin/data/"
  var mainData = "kopo_channel_seasonality_ex.csv"
  var subData = "kopo_product_mst.csv"

  // Dataframe
  var mainDataDf = spark.read.format("csv").
    option("header", "true").
    load(dataPath + mainData)

  var subDataDf = spark.read.format("csv").
    option("header", "true").
    load(dataPath + subData)

  mainDataDf.createOrReplaceTempView("mainTable")
  subDataDf.createOrReplaceTempView("subTable")

  mainDataDf.show(3)

    //a.productgroup b.productid
 spark.sql("select a.regionid, a.productgroup, b.productname, a.yearweek, a.qty " + // 끝에 띄어쓰기
   "from mainTable a " +
   "left join subTable b " +
   "on a.productgroup = b.productname"
 )

    var staticUrl = "jdbc:oracle:thin:@192.168.110.111:1521/orcl"
    var staticUser = "kopo"
    var staticPw = "kopo"
    var selloutDb = "kopo_channel_seasonality_new"

    val selloutDataFromMysql= spark.read.format("jdbc").
      options(Map("url" -> staticUrl,"dbtable" -> selloutDb,"user" -> staticUser, "password" -> staticPw)).load

    selloutDataFromMysql.createOrReplaceTempView("selloutTable")

    var staticUrl1 = "jdbc:oracle:thin:@192.168.110.111:1521/orcl"
    var staticUser1 = "kopo"
    var staticPw1 = "kopo"
    var selloutDb1 = "kopo_region_mst"

    val selloutDataFromMysql1= spark.read.format("jdbc").
      options(Map("url" -> staticUrl1,"dbtable" -> selloutDb1,"user" -> staticUser1, "password" -> staticPw1)).load

    selloutDataFromMysql1.createOrReplaceTempView("selloutTable1")

    spark.sql("select a.regionid, b.regionname, a.product, a.yearweek, a.qty " +
      "from selloutTable a " +
      "inner join selloutTable1 b " +
      "on a.regionid = b.regionid"
    )

  }
}
