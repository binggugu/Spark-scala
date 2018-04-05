package com.haiteam

import org.apache.spark.sql.SparkSession

object Example_05 {
  def main(args: Array[String]): Unit = {
    var spark = SparkSession.builder().appName("hkproject").
      config("spark.master", "local").
      getOrCreate()

    def quiz(inputValue:String):(Int,Int) = {

      var inputValue = "2017;34"
      var target = inputValue.split(";")
      var yearValue = target(0)
      var weekValue = target(1)
      (yearValue.toInt, weekValue.toInt)
    }
    var test = "2017;34"

    var answer = quiz(test)

    println(answer)

//    ------------------------------------------------------------------------------------------

    var staticUrl = "jdbc:oracle:thin:@192.168.110.111:1521/orcl"
    var staticUser = "kopo"
    var staticPw = "kopo"
    var selloutDb = "kopo_channel_seasonality_ex"

    val selloutDataFromMysql= spark.read.format("jdbc").
      options(Map("url" -> staticUrl,"dbtable" -> selloutDb,"user" -> staticUser, "password" -> staticPw)).load

    selloutDataFromMysql.createOrReplaceTempView("selloutTable")
    selloutDataFromMysql.show(2)

    selloutDataFromMysql.createTempView("maindata")

//    var tet = spark.sql("select * from maindata")

//    var tet = spark.sql("select cast(qty as double) as qty from maindata")

    var tet = spark.sql("select cast(qty as double) as qty2 from maindata")





  }
}
