package com.pingcap.spark

import java.util.Properties

import org.apache.spark.sql.SparkSession

/**
  * Created by ilovesoup1 on 26/07/2017.
  */
class Tpch13(spark: SparkSession, prop: Properties) extends Tpch(spark, prop) {

  override def sparkQuery =
    """select
      |	c_count,
      |	count(*) as custdist
      | from
      |	(
      |		select
      |			c_custkey,
      |			count(o_orderkey) as c_count
      |		from
      |			customer left outer join orders on
      |				c_custkey = o_custkey
      |				and o_comment not like '%pending%deposits%'
      |		group by
      |			c_custkey
      |	) c_orders
      | group by
      |	c_count
      | order by
      |	custdist desc,
      |	c_count desc
    	""".stripMargin

  override def tidbQuery =
    """
      select
 |	c_count,
 |	count(*) as custdist
 |from
 |	(
 |		select
 |			c_custkey,
 |			count(o_orderkey) as c_count
 |		from
 |			customer left outer join orders on
 |				c_custkey = o_custkey
 |				and o_comment not like '%pending%deposits%'
 |		group by
 |			c_custkey
 |	) c_orders
 |group by
 |	c_count
 |order by
 |	custdist desc,
 |	c_count desc;
    """.stripMargin
}
