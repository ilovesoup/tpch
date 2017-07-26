package com.pingcap.spark

import org.apache.spark.sql.SparkSession

/**
  * Created by ilovesoup1 on 26/07/2017.
  */
trait TiSparkTest {
  def test(spark: SparkSession): Boolean
}
