package com.pingcap.spark

import java.util.Properties

import org.apache.spark.sql.SparkSession

/**
  * Created by ilovesoup1 on 26/07/2017.
  */
class Tpch9(spark: SparkSession, prop: Properties) extends Tpch(spark, prop) {

  override def sparkQuery =
    """ select
      | 	nation,
      | 	o_year,
      | 	sum(amount) as sum_profit
      | from
      | 	(
      | 		select
      | 			n_name as nation,
      | 			year(o_orderdate) as o_year,
      | 			l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount
      | 		from
      | 			part,
      | 			supplier,
      | 			lineitem,
      | 			partsupp,
      | 			orders,
      | 			nation
      | 		where
      | 			s_suppkey = l_suppkey
      | 			and ps_suppkey = l_suppkey
      | 			and ps_partkey = l_partkey
      | 			and p_partkey = l_partkey
      | 			and o_orderkey = l_orderkey
      | 			and s_nationkey = n_nationkey
      | 			and p_name like '%dim%'
      | 	) as profit
      | group by
      | 	nation,
      | 	o_year
      | order by
      | 	nation,
      | 	o_year desc
    """.stripMargin

  override def tidbQuery =
    """
      select
 |	nation,
 |	o_year,
 |	sum(amount) as sum_profit
 |from
 |	(
 |		select
 |			n_name as nation,
 |			extract(year from o_orderdate) as o_year,
 |			l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount
 |		from
 |			part,
 |			supplier,
 |			lineitem,
 |			partsupp,
 |			orders,
 |			nation
 |		where
 |			s_suppkey = l_suppkey
 |			and ps_suppkey = l_suppkey
 |			and ps_partkey = l_partkey
 |			and p_partkey = l_partkey
 |			and o_orderkey = l_orderkey
 |			and s_nationkey = n_nationkey
 |			and p_name like '%dim%'
 |	) as profit
 |group by
 |	nation,
 |	o_year
 |order by
 |	nation,
 |	o_year desc;
    """.stripMargin
}
