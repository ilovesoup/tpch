package com.pingcap.spark

import java.util.Properties

import org.apache.spark.sql.{SparkSession, TiContext}

object TestFramework {
  val ConfName = "config.properties"


  def main(args: Array[String]) = {
    val spark = SparkSession
      .builder()
      .appName("TiSpark Integration Test")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val prop: Properties = loadConf(ConfName)
    val ti = new TiContext(spark, List(prop.getProperty("pdaddr")))
    ti.tidbMapDatabase("tpch")

    val tests: Array[Tpch] = Array(
      new Tpch1(spark, prop),
      new Tpch2(spark, prop),
      new Tpch3(spark, prop),
      new Tpch5(spark, prop),
      new Tpch6(spark, prop),
      new Tpch7(spark, prop),
      new Tpch8(spark, prop),
      new Tpch9(spark, prop),
      new Tpch10(spark, prop)
    )

    tests.foreach {
      test => {
        println("Test for " + test.testName())
        test.test()
        println("\n")
      }
    }
    spark.close()
    System.exit(0)
  }

  def loadConf(conf: String): Properties = {
    val confStream = getClass().getClassLoader().getResourceAsStream(conf)
    val prop = new Properties();
    prop.load(confStream)
    prop
  }
}
