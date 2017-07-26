package com.pingcap.spark

import java.util.Properties

import org.apache.spark.sql.SparkSession

/**
  * Created by ilovesoup1 on 26/07/2017.
  */
class Tpch5(spark: SparkSession, prop: Properties) extends Tpch(spark, prop) {
  override def testName() = "Tpch5"

  override def sparkQuery =
    """select
      |	n_name,
      |	sum(l_extendedprice * (1 - l_discount)) as revenue
      |from
      |	customer,
      |	orders,
      |	lineitem,
      |	supplier,
      |	nation,
      |	region
      |where
      |	c_custkey = o_custkey
      |	and l_orderkey = o_orderkey
      |	and l_suppkey = s_suppkey
      |	and c_nationkey = s_nationkey
      |	and s_nationkey = n_nationkey
      |	and n_regionkey = r_regionkey
      |	and r_name = 'ASIA'
      |	and o_orderdate >= date '1994-01-01'
      |	and o_orderdate < date '1994-01-01' + interval '1' year
      |group by
      |	n_name
      |order by
      |	revenue desc
    """.stripMargin

  override def tidbQuery =
    """
      select
 |           n_name,
 |           sum(l_extendedprice * (1 - l_discount)) as revenue
 |        from
 |           customer,
 |           orders,
 |           lineitem,
 |           supplier,
 |           nation,
 |           region
 |        where
 |           c_custkey = o_custkey
 |           and l_orderkey = o_orderkey
 |           and l_suppkey = s_suppkey
 |           and c_nationkey = s_nationkey
 |           and s_nationkey = n_nationkey
 |           and n_regionkey = r_regionkey
 |           and r_name = 'ASIA'
 |           and o_orderdate >= '1994-01-01'
 |           and o_orderdate < '1995-01-01'
 |        group by
 |           n_name
 |        order by
 |           revenue desc;
    """.stripMargin
}
