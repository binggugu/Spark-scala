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


  }
}
