/*
 *
 * Copyright 2017 PingCAP, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.pingcap.spark.testcase.tpch

import java.util.Properties

import com.pingcap.spark.TestBase
import org.apache.spark.sql.SparkSession


class Tpch2(spark: SparkSession, prop: Properties) extends TestBase(spark, prop) {
  override def tidbQuery =
    """
      select
 |        	s_acctbal,
 |        	s_name,
 |        	n_name,
 |        	p_partkey,
 |        	p_mfgr,
 |        	s_address,
 |        	s_phone,
 |        	s_comment
 |        from
 |        	part,
 |        	supplier,
 |        	partsupp,
 |        	nation,
 |        	region
 |        where
 |        	p_partkey = ps_partkey
 |        	and s_suppkey = ps_suppkey
 |        	and p_size = 15
 |        	and p_type like '%BRASS'
 |        	and s_nationkey = n_nationkey
 |        	and n_regionkey = r_regionkey
 |        	and r_name = 'EUROPE'
 |        	and ps_supplycost = (
 |        		select
 |        			min(ps_supplycost)
 |        		from
 |        			partsupp,
 |        			supplier,
 |        			nation,
 |        			region
 |        		where
 |        			p_partkey = ps_partkey
 |        			and s_suppkey = ps_suppkey
 |        			and s_nationkey = n_nationkey
 |        			and n_regionkey = r_regionkey
 |        			and r_name = 'EUROPE'
 |        	)
 |        order by
 |        	s_acctbal desc,
 |        	n_name,
 |        	s_name,
 |        	p_partkey
    """.stripMargin
}
