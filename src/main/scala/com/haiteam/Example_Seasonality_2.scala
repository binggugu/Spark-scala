package com.haiteam

import org.apache.spark.sql.SparkSession

object Example_Seasonality_2 {
  def main(args: Array[String]): Unit = {
    var spark = SparkSession.builder().appName("hkproject").
      config("spark.master", "local").
      getOrCreate()

    var staticUrl = "jdbc:oracle:thin:@192.168.110.111:1521/orcl"

    var staticUser = "kpo"
    var staticPw = "kopo"
    var selloutDb = "kopo_channel_seasonality_new"
    var productNameDb = "kopo_product_mst"

    val selloutDf = spark.read.format("jdbc").
      options(Map("url" -> staticUrl, "dbtable" -> selloutDb,
        "user" -> staticUser,
        "password" -> staticPw)).load

    val productMasterDf = spark.read.format("jdbc").
      options(Map("url" -> staticUrl, "dbtable" -> productNameDb,
        "user" -> staticUser,
        "password" -> staticPw)).load

    selloutDf.createOrReplaceTempView("selloutTable")
    productMasterDf.createOrReplaceTempView("mstTable")

    var rawData = spark.sql("select " + // 나머지는 string 4번재 cast는 double
      "concat(a.regionid,'_',a.product) as keycol, " +
      "a.regionid as accountid, " +
      "a.product, " +
      "a.yearweek, " +
      "cast(a.qty as double) as qty, " +
      "b.product_name " +
      "from selloutTable a " +
      "left join mstTable b " +
      "on a.product = b.product_id")

    rawData.show(2)

    var rawDataColumns = rawData.columns
    var keyNo = rawDataColumns.indexOf("keycol")
    var accountidNo = rawDataColumns.indexOf("accountid")
    var productNo = rawDataColumns.indexOf("product")
    var yearweekNo = rawDataColumns.indexOf("yearweek")
    var qtyNo = rawDataColumns.indexOf("qty")
    var productnameNo = rawDataColumns.indexOf("product_name")

    var rawRdd = rawData.rdd

    // (kecol, accountid, product, yearweek, qty, product_name)
    var rawExRdd = rawRdd.filter(x=>{
      var checkValid = true
      // 설정 부적합 로직
      if(x.getString(yearweekNo).length != 6){
        checkValid = false
      }
      checkValid
    })

    //A60 PRODUCT34 201402 4463
    // 랜덤 디버깅 Case #1
    var x = rawRdd.first
    // 디버깅 Case #2 (타겟팅 대상 선택)
    var rawExRdd5 = rawRdd.filter(x=>{
      var checkValid = false
      if( (x.getString(accountidNo) == "A60") &&
        (x.getString(productNo) == "PRODUCT34") &&
        (x.getString(yearweekNo) == "201553") ){
        checkValid = true
      }
      checkValid
    })

    // var x = rawExRdd5.first
    var rawExRdd3 = rawRdd.filter(y=>{
      var checkValid = true
      // 52주차 삭제 로직
      if( y.getString(yearweekNo).substring(4).toInt > 52 ){
        checkValid = false
      }
      checkValid
    })



  }

}
