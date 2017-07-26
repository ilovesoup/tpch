package com.pingcap.spark

import java.util.Properties

import org.apache.spark.sql.SparkSession

/**
  * Created by ilovesoup1 on 26/07/2017.
  */
class Tpch14(spark: SparkSession, prop: Properties) extends Tpch(spark, prop) {

  override def sparkQuery =
    """
      |select
      |	100.00 * sum(case
      |		when p_type like 'PROMO%'
      |			then l_extendedprice * (1 - l_discount)
      |		else 0
      |	end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue
      |from
      |	lineitem,
      |	part
      |where
      |	l_partkey = p_partkey
      |	and l_shipdate >= date '1995-09-01'
      |	and l_shipdate < date '1995-09-01' + interval '1' month
    """.stripMargin

  override def tidbQuery =
    """

 |        select
 |        	100.00 * sum(case
 |        		when p_type like 'PROMO%'
 |        			then l_extendedprice * (1 - l_discount)
 |        		else 0
 |        	end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue
 |        from
 |        	lineitem,
 |        	part
 |        where
 |        	l_partkey = p_partkey
 |        	and l_shipdate >= '1995-09-01'
 |        	and l_shipdate < '1995-10-01';
    """.stripMargin
}
