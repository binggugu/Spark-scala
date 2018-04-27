package com.haiteam

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}


object TestFile {
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
      "REGIONID, "+
      "PRODUCT, "+
      "YEARWEEK, "+
      "cast(QTY as double), "+
      "cast(QTY * 1.2 as double)as QTY_NEW "+
      "from selloutTable")

    ///////////////////////////////////////// 3번답
//    자바 이클립스 에서
//    public static int getweekinfo (String inputValue){
//      int result = Integer.parseInt(inputValue.substring(4));
//      return result
//    } + main 추가 + import 해서 com.hk.jar 파일을 c:/spark/jars에 넣고
//      cmd – import com.hk.파일명 – com.hk.파일명.getweekinfo(“201612”)

//////////////////////////////////////// 5번
    rawData.show(2)

    var rawDataColumns = rawData.columns
    var keyNo = rawDataColumns.indexOf("KEYCOL")
    var accountidNo = rawDataColumns.indexOf("REGIONID")
    var productNo = rawDataColumns.indexOf("PRODUCT")
    var yearweekNo = rawDataColumns.indexOf("YEARWEEK")
    var qtyNo = rawDataColumns.indexOf("QTY")
    var productnameNo = rawDataColumns.indexOf("QTY_NEW")

    /////////////////RDD로 변환
    var rawRdd = rawData.rdd

    /////////////////////RDD로 변환후 정제하기

    /////////////// 2016년도 이상 정제

    var filteredRdd = rawRdd.filter(x=>{
      var checkValid = false
      var yearValue = x.getString(yearweekNo).substring(0,4).toInt

      if( yearValue >= 2016 ){
        checkValid = true
      }
      checkValid
    })

    /////////// 53주차 미포함 201652 201611

    var filteredRdd2 = filteredRdd.filter(x=> {
      var checkValid = true
      var weekValue = x.getString(yearweekNo).substring(4,6).toInt //substring 4이상은 전부다 포함

      if (weekValue == 53){
        checkValid = false
      }
      checkValid
    })


    ////// 프로덕트 (product1,product2) 정보만
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
    resultRdd.take(3).foreach(println) // 3개만 보이는것
    resultRdd.collect.toArray.foreach(println) // 전체 보이는것

    /////////////////////////////// 6번

    //////////// RDD 에서 Dataframe 에서 import 해주기
    // import org.apache.spark.sql.types.{StringType, DoubleType, StructField, StructType}

    val finalResultDf = spark.createDataFrame(resultRdd,
      StructType(
        Seq(
          StructField("KEYCOL", StringType),
        StructField("REGIONID", StringType),
        StructField("PRODUCT", StringType),
        StructField("YEARWEEK", StringType),
        StructField("QTY", DoubleType),
        StructField("QTY_NEW", DoubleType))))

    ////////// 데이터 저장 (데엍 베이스 안에 저장할 때)
    var myUrl = "jdbc:oracle:thin:@192.168.110.111:1521/orcl"
    var myUrl1 = "jdbc:oracle:thin:@192.168.110.20:1522/XE"

    /////POSTGRES 서버 or 안에 데이터 저장하기
    var staticUrl1 = "jdbc:postgresql://192.168.110.111:5432/kopo"
    var staticUrl2 = "jdbc:postgresql://127.0.0.1:5432/postgres"
    var outputUrl = "jdbc:postgre_kopo://127.0.0.1:5432/postgres"

    val prop = new java.util.Properties
    prop.setProperty("driver", "oracle.jdbc.OracleDriver")
    prop.setProperty("user", "kimjisung")
    prop.setProperty("password", "kimjisung")
    val table = "KOPO_ST_MIDDLE_KJS"
    //append
    finalResultDf.write.mode("overwrite").jdbc(myUrl1, table, prop)

    /////// 드라이브
//    driver : org.postgresql.Driver  ========= postgres
//      driver : oracle.jdbc.OracleDriver =========== Oracle


  }
}
