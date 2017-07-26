package com.pingcap.spark

import java.util.Properties

import org.apache.spark.sql.SparkSession

/**
  * Created by ilovesoup1 on 26/07/2017.
  */
class Tpch12(spark: SparkSession, prop: Properties) extends Tpch(spark, prop) {

  override def sparkQuery =
    """select
      |	l_shipmode,
      |	sum(case
      |		when o_orderpriority = '1-URGENT'
      |			or o_orderpriority = '2-HIGH'
      |			then 1
      |		else 0
      |	end) as high_line_count,
      |	sum(case
      |		when o_orderpriority <> '1-URGENT'
      |			and o_orderpriority <> '2-HIGH'
      |			then 1
      |		else 0
      |	end) as low_line_count
      |from
      |	orders,
      |	lineitem
      |where
      |	o_orderkey = l_orderkey
      |	and l_shipmode in ('MAIL', 'SHIP')
      |	and l_commitdate < l_receiptdate
      |	and l_shipdate < l_commitdate
      |	and l_receiptdate >= date '1994-01-01'
      |	and l_receiptdate < date '1994-01-01' + interval '1' year
      |group by
      |	l_shipmode
      |order by
      |	l_shipmode
    """.stripMargin

  override def tidbQuery =
    """
      |select
      |        	l_shipmode,
      |        	sum(case
      |        		when o_orderpriority = '1-URGENT'
      |        			or o_orderpriority = '2-HIGH'
      |        			then 1
      |        		else 0
      |        	end) as high_line_count,
      |        	sum(case
      |        		when o_orderpriority <> '1-URGENT'
      |        			and o_orderpriority <> '2-HIGH'
      |        			then 1
      |        		else 0
      |        	end) as low_line_count
      |        from
      |        	orders,
      |        	lineitem
      |        where
      |        	o_orderkey = l_orderkey
      |        	and l_shipmode in ('MAIL', 'SHIP')
      |        	and l_commitdate < l_receiptdate
      |        	and l_shipdate < l_commitdate
      |        	and l_receiptdate >= '1994-01-01'
      |        	and l_receiptdate < '1995-01-01'
      |        group by
      |        	l_shipmode
      |        order by
      |        	l_shipmode;
    """.stripMargin
}
