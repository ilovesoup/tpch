package com.pingcap.spark

import java.util.Properties

import org.apache.spark.sql.SparkSession

/**
  * Created by ilovesoup1 on 26/07/2017.
  */
class Tpch22(spark: SparkSession, prop: Properties) extends Tpch(spark, prop) {

  override def sparkQuery =
    """
      |      select
      |	cntrycode,
      |	count(*) as numcust,
      |	sum(c_acctbal) as totacctbal
      | from
      |	(
      |		select
      |			substring(c_phone, 1 , 2) as cntrycode,
      |			c_acctbal
      |		from
      |			customer
      |		where
      |			substring(c_phone , 1 , 2) in
      |				('20', '40', '22', '30', '39', '42', '21')
      |			and c_acctbal > (
      |				select
      |					avg(c_acctbal)
      |				from
      |					customer
      |				where
      |					c_acctbal > 0.00
      |					and substring(c_phone , 1 , 2) in
      |						('20', '40', '22', '30', '39', '42', '21')
      |			)
      |			and not exists (
      |				select
      |					*
      |				from
      |					orders
      |				where
      |					o_custkey = c_custkey
      |			)
      |	) as custsale
      | group by
      |	cntrycode
      | order by
      |	cntrycode
    	""".stripMargin

  override def tidbQuery =
    """
       select
 |	cntrycode,
 |	count(*) as numcust,
 |	sum(c_acctbal) as totacctbal
 | from
 |	(
 |		select
 |			substring(c_phone, 1 , 2) as cntrycode,
 |			c_acctbal
 |		from
 |			customer
 |		where
 |			substring(c_phone , 1 , 2) in
 |				('20', '40', '22', '30', '39', '42', '21')
 |			and c_acctbal > (
 |				select
 |					avg(c_acctbal)
 |				from
 |					customer
 |				where
 |					c_acctbal > 0.00
 |					and substring(c_phone , 1 , 2) in
 |						('20', '40', '22', '30', '39', '42', '21')
 |			)
 |			and not exists (
 |				select
 |					*
 |				from
 |					orders
 |				where
 |					o_custkey = c_custkey
 |			)
 |	) as custsale
 | group by
 |	cntrycode
 | order by
 |	cntrycode;
    """.stripMargin
}
