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


class Tpch16(spark: SparkSession, prop: Properties) extends TestBase(spark, prop) {

  override def tidbQuery =
    """
 |                select
 |        	p_brand,
 |        	p_type,
 |        	p_size,
 |        	count(distinct ps_suppkey) as supplier_cnt
 |        from
 |        	partsupp,
 |        	part
 |        where
 |        	p_partkey = ps_partkey
 |        	and p_brand <> 'Brand#45'
 |        	and p_type not like 'MEDIUM POLISHED%'
 |        	and p_size in (49, 14, 23, 45, 19, 3, 36, 9)
 |        	and ps_suppkey not in (
 |        		select
 |        			s_suppkey
 |        		from
 |        			supplier
 |        		where
 |        			s_comment like '%Customer%Complaints%'
 |        	)
 |        group by
 |        	p_brand,
 |        	p_type,
 |        	p_size
 |        order by
 |        	supplier_cnt desc,
 |        	p_brand,
 |        	p_type,
 |        	p_size
    """.stripMargin
}
