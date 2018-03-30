package com.haiteam

import org.apache.spark.sql.SparkSession

object Example_02 {
  def main(args: Array[String]): Unit = {
    var spark = SparkSession.builder().appName("hkproject").
      config("spark.master", "local").
      getOrCreate()

    //접속정보 설정
    var staticUrl = "jdbc:postgresql://192.168.110.111:5432/kopo"
    var staticUser = "kopo"
    var staticPw = "kopo"
    var selloutDb = "kopo_channel_seasonality"

    // jdbc (java database connectivity) 연결
    val selloutDataFromPg= spark.read.format("jdbc").
      options(Map("url" -> staticUrl,"dbtable" -> selloutDb,"user" -> staticUser, "password" -> staticPw)).load

    //메모리 테이블 생성 (가상의 테이블을 생성)
    selloutDataFromPg.createOrReplaceTempView("selloutTable") // 똑같은 이름으로 쓰면 에러가 난다.

    selloutDataFromPg.show(1)

  }
}
