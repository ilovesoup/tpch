package com.pingcap.spark

import java.util.Properties

import org.apache.spark.sql.SparkSession

/**
  * Created by ilovesoup1 on 26/07/2017.
  */
class Tpch1(spark: SparkSession, prop: Properties) extends Tpch(spark, prop) {
  override def sparkQuery =
    """select
      |   l_returnflag,
      |   l_linestatus,
      |   sum(l_quantity) as sum_qty,
      |   sum(l_extendedprice) as sum_base_price,
      |   sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
      |   sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
      |   avg(l_quantity) as avg_qty,
      |   avg(l_extendedprice) as avg_price,
      |   avg(l_discount) as avg_disc,
      |   count(*) as count_order
      |from
      |   lineitem
      |where
      |   l_shipdate <= date '1998-12-01' - interval '90' day
      |group by
      |   l_returnflag,
      |   l_linestatus
      |order by
      |   l_returnflag,
      |   l_linestatus
    """.stripMargin

  override def tidbQuery =
    """
      select
      |           l_returnflag,
      |           l_linestatus,
      |           sum(l_quantity) as sum_qty,
      |           sum(l_extendedprice) as sum_base_price,
      |           sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
      |           sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
      |           avg(l_quantity) as avg_qty,
      |           avg(l_extendedprice) as avg_price,
      |           avg(l_discount) as avg_disc,
      |           count(*) as count_order
      |        from
      |           lineitem
      |        where
      |           l_shipdate <= '1998-09-02'
      |        group by
      |           l_returnflag,
      |           l_linestatus
      |        order by
      |           l_returnflag,
      |           l_linestatus;
    """.stripMargin
}
