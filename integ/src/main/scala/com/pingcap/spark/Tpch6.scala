package com.pingcap.spark

import java.util.Properties

import org.apache.spark.sql.SparkSession

/**
  * Created by ilovesoup1 on 26/07/2017.
  */
class Tpch6(spark: SparkSession, prop: Properties) extends Tpch(spark, prop) {
  override def testName() = "Tpch6"

  override def sparkQuery =
    """select
      |	sum(l_extendedprice * l_discount) as revenue
      |from
      |	lineitem
      |where
      |	l_shipdate >= date '1994-01-01'
      |	and l_shipdate < date '1994-01-01' + interval '1' year
      |	and l_discount between .06 - 0.01 and .06 + 0.01
      |	and l_quantity < 24
    """.stripMargin

  override def tidbQuery =
    """
      |select
      |           sum(l_extendedprice * l_discount) as revenue
      |        from
      |           lineitem
      |        where
      |           l_shipdate >= '1994-01-01'
      |           and l_shipdate < '1995-01-01'
      |           and l_discount between .06 - 0.01 and .06 + 0.01
      |           and l_quantity < 24;
    """.stripMargin
}
