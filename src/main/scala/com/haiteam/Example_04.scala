package com.haiteam

import org.apache.spark.sql.SparkSession

object Example_04 {
    def main(args: Array[String]): Unit = {

      var spark = SparkSession.builder().appName("hkproject").
        config("spark.master", "local").
        getOrCreate()

      var priceData = Array(1000d,1200d,1300d,1500d,10000d)
      var promitionRate = 0.2
      var priceDataSize = priceData.size
      var i = 0

      while(i < priceDataSize){
        // 할인가격계산 : 할인가격 = 실제가격 * 할인비율
        var promotionEffect = priceData(i) * promitionRate
        // 최종결과계산 : 최종가격은 = 가격 - 할인가격
        priceData(i) = priceData(i) - promotionEffect
        // 조건 대상 증가
        i = i+1
      }




    }
}
