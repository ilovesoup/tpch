package com.pingcap.spark

import java.util.Properties

import org.apache.spark.sql.SparkSession

/**
  * Created by ilovesoup1 on 26/07/2017.
  */
class Tpch11(spark: SparkSession, prop: Properties) extends Tpch(spark, prop) {

  override def sparkQuery =
    """select
      |	ps_partkey,
      |	sum(ps_supplycost * ps_availqty) as value
      |from
      |	partsupp,
      |	supplier,
      |	nation
      |where
      |	ps_suppkey = s_suppkey
      |	and s_nationkey = n_nationkey
      |	and n_name = 'GERMANY'
      |group by
      |	ps_partkey having
      |		sum(ps_supplycost * ps_availqty) > (
      |			select
      |				sum(ps_supplycost * ps_availqty) * 0.0001000000
      |			from
      |				partsupp,
      |				supplier,
      |				nation
      |			where
      |				ps_suppkey = s_suppkey
      |				and s_nationkey = n_nationkey
      |				and n_name = 'GERMANY'
      |		)
      |order by
      |	value desc
    """.stripMargin

  override def tidbQuery =
    """
      select
 |        	ps_partkey,
 |        	sum(ps_supplycost * ps_availqty) as value
 |        from
 |        	partsupp,
 |        	supplier,
 |        	nation
 |        where
 |        	ps_suppkey = s_suppkey
 |        	and s_nationkey = n_nationkey
 |        	and n_name = 'GERMANY'
 |        group by
 |        	ps_partkey having
 |        		sum(ps_supplycost * ps_availqty) > (
 |        			select
 |        				sum(ps_supplycost * ps_availqty) * 0.0001000000
 |        			from
 |        				partsupp,
 |        				supplier,
 |        				nation
 |        			where
 |        				ps_suppkey = s_suppkey
 |        				and s_nationkey = n_nationkey
 |        				and n_name = 'GERMANY'
 |        		)
 |        order by
 |        	value desc;
    """.stripMargin
}
