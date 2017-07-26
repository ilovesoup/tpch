package com.pingcap.spark

import java.util.Properties

import org.apache.spark.sql.SparkSession

/**
  * Created by ilovesoup1 on 26/07/2017.
  */
class Tpch21(spark: SparkSession, prop: Properties) extends Tpch(spark, prop) {

  override def sparkQuery =
    """
      |select
      |	s_name,
      |	count(*) as numwait
      |from
      |	supplier,
      |	lineitem l1,
      |	orders,
      |	nation
      |where
      |	s_suppkey = l1.l_suppkey
      |	and o_orderkey = l1.l_orderkey
      |	and o_orderstatus = 'F'
      |	and l1.l_receiptdate > l1.l_commitdate
      |	and exists (
      |		select
      |			*
      |		from
      |			lineitem l2
      |		where
      |			l2.l_orderkey = l1.l_orderkey
      |			and l2.l_suppkey <> l1.l_suppkey
      |	)
      |	and not exists (
      |		select
      |			*
      |		from
      |			lineitem l3
      |		where
      |			l3.l_orderkey = l1.l_orderkey
      |			and l3.l_suppkey <> l1.l_suppkey
      |			and l3.l_receiptdate > l3.l_commitdate
      |	)
      |	and s_nationkey = n_nationkey
      |	and n_name = 'SAUDI ARABIA'
      |group by
      |	s_name
      |order by
      |	numwait desc,
      |	s_name
    """.stripMargin

  override def tidbQuery =
    """
 |        select
 |        	s_name,
 |        	count(*) as numwait
 |        from
 |        	supplier,
 |        	lineitem l1,
 |        	orders,
 |        	nation
 |        where
 |        	s_suppkey = l1.l_suppkey
 |        	and o_orderkey = l1.l_orderkey
 |        	and o_orderstatus = 'F'
 |        	and l1.l_receiptdate > l1.l_commitdate
 |        	and exists (
 |        		select
 |        			*
 |        		from
 |        			lineitem l2
 |        		where
 |        			l2.l_orderkey = l1.l_orderkey
 |        			and l2.l_suppkey <> l1.l_suppkey
 |        	)
 |        	and not exists (
 |        		select
 |        			*
 |        		from
 |        			lineitem l3
 |        		where
 |        			l3.l_orderkey = l1.l_orderkey
 |        			and l3.l_suppkey <> l1.l_suppkey
 |        			and l3.l_receiptdate > l3.l_commitdate
 |        	)
 |        	and s_nationkey = n_nationkey
 |        	and n_name = 'SAUDI ARABIA'
 |        group by
 |        	s_name
 |        order by
 |        	numwait desc,
 |        	s_name;
    """.stripMargin
}
