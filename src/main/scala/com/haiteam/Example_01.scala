package com.haiteam

import org.apache.spark.sql.SparkSession

object Example_01 {
  def main(args: Array[String]): Unit = {

    var spark = SparkSession.builder().appName("hkproject").
      config("spark.master", "local").
      getOrCreate()

    var a = 10
    println(10)

    var testArray = Array(22, 33, 50, 70, 90, 100)

    var answer = testArray.filter(x => {
      x % 10 == 0
    })

    var ansewr = testArray.filter(x => {
      var data = x.toString
      var dataSize = data.size

      var lastChar = data.substring(dataSize - 1).toString

      lastChar.equalsIgnoreCase("0")
    })

    var arraySize = answer.size
    for (i <- 0 until arraySize) {
      println(answer(i))



      var staticUrl3 = "jdbc:oracle:thin:@192.168.110.20:1522/XE"
      var staticUser3 = "KIMJISUNG"
      var staticPw3 = "kimjisung"
      var selloutDb3 = "KOPO_PRODUCT_VOLUME"

      val selloutDataFromMysql3= spark.read.format("jdbc").
        options(Map("url" -> staticUrl3,"dbtable" -> selloutDb3,"user" -> staticUser3, "password" -> staticPw3)).load

      selloutDataFromMysql3.createOrReplaceTempView("selloutTable")
      selloutDataFromMysql3.show(2)

    }

  }
}
