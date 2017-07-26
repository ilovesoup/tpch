package com.pingcap.spark

import java.util.Properties

import org.apache.spark.sql.SparkSession

/**
  * Created by ilovesoup1 on 26/07/2017.
  */
class Tpch18(spark: SparkSession, prop: Properties) extends Tpch(spark, prop) {

  override def sparkQuery =
    """
      |select
      |	c_name,
      |	c_custkey,
      |	o_orderkey,
      |	o_orderdate,
      |	o_totalprice,
      |	sum(l_quantity)
      |from
      |	customer,
      |	orders,
      |	lineitem
      |where
      |	o_orderkey in (
      |		select
      |			l_orderkey
      |		from
      |			lineitem
      |		group by
      |			l_orderkey having
      |				sum(l_quantity) > 300
      |	)
      |	and c_custkey = o_custkey
      |	and o_orderkey = l_orderkey
      |group by
      |	c_name,
      |	c_custkey,
      |	o_orderkey,
      |	o_orderdate,
      |	o_totalprice
      |order by
      |	o_totalprice desc,
      |	o_orderdate
    """.stripMargin

  override def tidbQuery =
    """
 select
 |            	c_name,
 |            	c_custkey,
 |            	o_orderkey,
 |            	o_orderdate,
 |            	o_totalprice,
 |            	sum(l_quantity)
 |            from
 |            	customer,
 |            	orders,
 |            	lineitem
 |            where
 |            	o_orderkey in (
 |            		select
 |            			l_orderkey
 |            		from
 |            			lineitem
 |            		group by
 |            			l_orderkey having
 |            				sum(l_quantity) > 300
 |            	)
 |            	and c_custkey = o_custkey
 |            	and o_orderkey = l_orderkey
 |            group by
 |            	c_name,
 |            	c_custkey,
 |            	o_orderkey,
 |            	o_orderdate,
 |            	o_totalprice
 |            order by
 |            	o_totalprice desc,
 |            	o_orderdate;
    """.stripMargin
}
