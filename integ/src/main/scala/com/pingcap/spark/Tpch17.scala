package com.pingcap.spark

import java.util.Properties

import org.apache.spark.sql.SparkSession

/**
  * Created by ilovesoup1 on 26/07/2017.
  */
class Tpch17(spark: SparkSession, prop: Properties) extends Tpch(spark, prop) {

  override def sparkQuery =
    """
      |select
      |	sum(l_extendedprice) / 7.0 as avg_yearly
      |from
      |	lineitem,
      |	part
      |where
      |	p_partkey = l_partkey
      |	and p_brand = 'Brand#23'
      |	and p_container = 'MED BOX'
      |	and l_quantity < (
      |		select
      |			0.2 * avg(l_quantity)
      |		from
      |			lineitem
      |		where
      |			l_partkey = p_partkey
      |	)
    """.stripMargin

  override def tidbQuery =
    """
 select
 |    sum(l_extendedprice) / 7.0 as avg_yearly
 |            from
 |            	lineitem,
 |            	part
 |            where
 |            	p_partkey = l_partkey
 |            	and p_brand = 'Brand#23'
 |            	and p_container = 'MED BOX'
 |            	and l_quantity < (
 |            		select
 |            			0.2 * avg(l_quantity)
 |            		from
 |            			lineitem
 |            		where
 |            			l_partkey = p_partkey
 |            	);
    """.stripMargin
}
