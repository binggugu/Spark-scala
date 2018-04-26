package com.haiteam

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

object Test {
  def main(args: Array[String]): Unit = {
    var spark = SparkSession.builder().appName("hkproject").
      config("spark.master", "local").
      getOrCreate()

    ///////////////////////////////////////// 1번답
    var staticUrl = "jdbc:oracle:thin:@192.168.110.112:1521/orcl"
    var staticUser = "kopo"
    var staticPw = "kopo"
    var selloutDb = "kopo_channel_seasonality_new"
    val selloutDf = spark.read.format("jdbc").
      options(Map("url" -> staticUrl, "dbtable" -> selloutDb,
        "user" -> staticUser,
        "password" -> staticPw)).load

    selloutDf.createOrReplaceTempView("selloutTable")
    selloutDf.show(2)

    ///////////////////////////////////////// 2번답
    var rawData = spark.sql ("select "+
      "regionid, "+
      "product, "+
      "yearweek, "+
      "cast(qty as double), "+
      "cast(qty * 1.2 as double)as qty_new "+
      "from selloutTable")

    //////////////////////////////////////// 4번답
    var rawDataColumns = rawData.columns
    var rdgionidNo = rawDataColumns.indexOf("regionid")
    var productNo = rawDataColumns.indexOf("product")
    var yearweekNo = rawDataColumns.indexOf("yearweek")
    var qtyNo = rawDataColumns.indexOf("qty")
    var productnameNo = rawDataColumns.indexOf("qty_new")

    var rawRdd = rawData.rdd

    var filteredRdd = rawRdd.filter(x=>{
      var checkValid = false
      var yearValue = x.getString(yearweekNo).substring(0,4).toInt

      if( yearValue >= 2016 ){
        checkValid = true
      }
      checkValid
    })

    var filteredRdd2 = filteredRdd.filter(x=> {
      var checkValid = true
      var weekValue = x.getString(yearweekNo).substring(4,6).toInt //substring 4이상은 전부다 포함

      if (weekValue == 52){
        checkValid = false
      }
      checkValid
    })

    var productArray = Array("PRODUCT1","PRODUCT2")
    var productSet = productArray.toSet

    var resultRdd = filteredRdd2.filter(x=>{
      var checkValid = false
      var productInfo = x.getString(productNo)
      if ( (productInfo == "PRODUCT1") ||
        (productInfo == "PRODUCT2")){
        checkValid = true
      }
      checkValid
    })

    resultRdd.first

    /////////////////////////////// 5번답

    val finalResultDf = spark.createDataFrame(resultRdd,
      StructType(
        Seq(
          StructField("regionid", StringType),
          StructField("product", StringType),
          StructField("yearweek", StringType),
          StructField("qty", DoubleType),
          StructField("qty_new", DoubleType))))

    var staticUrl1 = "jdbc:postgresql://192.168.110.111:5432/kopo"

    val prop = new java.util.Properties
    prop.setProperty("driver", "org.postgresql.Driver")
    prop.setProperty("user", "kopo")
    prop.setProperty("password", "kopo")
    val table = "kopo_st_result_kjs"
    //append
    finalResultDf.write.mode("overwrite").jdbc(staticUrl1, table, prop)

}
