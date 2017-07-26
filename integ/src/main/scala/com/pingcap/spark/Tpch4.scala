package com.pingcap.spark

import java.util.Properties

import org.apache.spark.sql.SparkSession

/**
  * Created by ilovesoup1 on 26/07/2017.
  */
class Tpch4(spark: SparkSession, prop: Properties) extends Tpch(spark, prop) {
  override def sparkQuery =
    """select
      |	o_orderpriority,
      |	count(*) as order_count
      |from
      |	orders
      |where
      |	o_orderdate >= date '1993-07-01'
      |	and o_orderdate < date '1993-07-01' + interval '3' month
      |	and exists (
      |		select
      |			*
      |		from
      |			lineitem
      |		where
      |			l_orderkey = o_orderkey
      |			and l_commitdate < l_receiptdate
      |	)
      |group by
      |	o_orderpriority
      |order by
      |	o_orderpriority
    """.stripMargin

  override def tidbQuery =
    """
      select
 |        	o_orderpriority,
 |        	count(*) as order_count
 |        from
 |        	orders
 |        where
 |        	o_orderdate >= '1993-07-01'
 |        	and o_orderdate < '1993-10-01'
 |        	and exists (
 |        		select
 |        			*
 |        		from
 |        			lineitem
 |        		where
 |        			l_orderkey = o_orderkey
 |        			and l_commitdate < l_receiptdate
 |        	)
 |        group by
 |        	o_orderpriority
 |        order by
 |        	o_orderpriority;
    """.stripMargin
}
