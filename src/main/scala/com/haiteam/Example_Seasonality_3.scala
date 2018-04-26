package com.haiteam

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.Row

object Example_Seasonality_3 {
  def main(args: Array[String]): Unit = {
    var spark = SparkSession.builder().appName("hkproject").
      config("spark.master", "local").
      getOrCreate()

    // 자신이 생성한 RDD에 연주자 정보가 52보다 큰값은 제거하는 로직 구현
    // oracle connection
    var staticUrl = "jdbc:oracle:thin:@192.168.110.111:1521/orcl"
    var staticUser = "kopo"
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

    var rawData = spark.sql("select " +
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

    // (kecol, accountid, product, yearweek, qty, product_name)
    var rawRdd = rawData.rdd

    var filteredRdd = rawRdd.filter(x=>{
      // boolean = true
      var checkValid = true
      // 찾기: yearweek 인덱스로 주차정보만 인트타입으로 변환
      var weekValue = x.getString(yearweekNo).substring(4).toInt

      // 비교한후 주차정보가 53 이상인 경우 레코드 삭제
      if( weekValue >= 53 ){
        checkValid = false
      }
      checkValid
    })
    rawRdd.take(3).foreach(println) // 3개만 보이는것
    rawRdd.collect.toArray.foreach(println) // 전체 보이는것


    // ---------------------------------------------------------------------------------
    // 상품정보가 PRODUCT1,2 인 정보만 필터링 하세요.

    // 분석대상 제품군 등록
    var productArray = Array("PRODUCT1","PRODUCT2")

    // 세트 타입으로 변환 // contains를 쓰기 위해서 productArray를 toSet으로 변경해준다.
    var productSet = productArray.toSet

    var resultRdd = filteredRdd.filter(x=>{
      var checkValid = false

      // 데이터 특정 행의 product 컬럼인덱스를 활용하여 데이터 대입
      var productInfo = x.getString(productNo)

      if(productSet.contains(productInfo)){
        //contains 는 productSet에 진실인가 거짓인가를 판단하는 명령어고 productInfo는 product
        checkValid = true
      }

      // 2번째 답!!!
      if ( (productInfo == "PRODUCT1") ||   //productArray(0)
           (productInfo == "PRODUCT2")){    //productArray(1)
        checkValid = true
      }
      checkValid
    })

    resultRdd.first
    resultRdd.take(3).foreach(println) // 3개만 보이는것
    resultRdd.collect.toArray.foreach(println) // 전체 보이는것

    // ------------------------------------------------------------------------
    val middleResult = spark.createDataFrame(resultRdd,
      StructType(
        Seq(
          StructField("1", StringType),
          StructField("2", StringType),
          StructField("3", StringType),
          StructField("4", StringType),
          StructField("5", DoubleType),
          StructField("6", StringType))))

    var mapRdd = rawRdd.map(x=>{
      var qty = x.getDouble(qtyNo)
      if(qty > 700000){qty = 700000}
      Row( x.getString(keyNo),
        x.getString(yearweekNo),
        qty) //x.getString(qtyNo)
    })

    // 처리로직 : 거래량이 MAXVALUE 이상인건은 MAXVALUE로 치환한다.
    var MAXVALUE = 700000 // 맨위에다가 배치, Global Variable Definition

    var mapRdd2 = filteredRdd.map(x=>{
      // 디버깅 코드 : var x = mapRdd.filter(x=>{x.getDouble(qtyNo) > 700000 }).first
      // 로직구현예정
      var org_qty = x.getDouble(qtyNo)
      var new_qty = org_qty

      if(new_qty > MAXVALUE){
       new_qty = MAXVALUE
      }

      // 출력 row 키정보,  연주차정보, 거래량 정보_org, 거래량 정보_new )
      (x.getString(keyNo),
       x.getString(yearweekNo),
       org_qty,
       new_qty)
    })





  }
}
