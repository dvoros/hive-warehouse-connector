/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hortonworks.spark.sql.hive.llap

import java.util.{HashMap, UUID}

import com.hortonworks.spark.sql.hive.llap.pushdowns.PushDownUtil
import org.apache.spark.sql.catalyst.analysis.{Analyzer, EmptyFunctionRegistry}
import org.apache.spark.sql.catalyst.catalog.{InMemoryCatalog, SessionCatalog}
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.{CASE_SENSITIVE, GROUP_BY_ORDINAL}
import org.apache.spark.sql.types._
import org.scalatest.FunSuite

class TestCatalystPushdown extends FunSuite with PlanTest {
  val conf = new SQLConf().copy(CASE_SENSITIVE -> false, GROUP_BY_ORDINAL -> false)
  val catalog = new SessionCatalog(new InMemoryCatalog, EmptyFunctionRegistry, conf)
  val analyzer = new Analyzer(catalog, conf)

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches = Batch("CatalystPushdowns", FixedPoint(100),
      GlobalLimitRule(null)) :: Nil
  }

  private val options = new HashMap[String, String]()
  options.put(HiveWarehouseSessionImpl.HWC_SESSION_ID_KEY, UUID.randomUUID().toString);
  val testRelation = DataSourceV2Relation(
    AttributeReference("a", IntegerType)() :: Nil,
    new HiveWarehouseDataSourceReader(options)
  )

  test("Global limit over Reader") {
    val limit = 10
    val query = testRelation.select('a).limit(limit)
    val optimized = Optimize.execute(analyzer.execute(query))
    val correctAnswer = testRelation.select('a).analyze

    comparePlans(optimized, correctAnswer)

    val originalSql = "select * from foo";
    val transformedSql = PushDownUtil.pushDown(originalSql,
      testRelation.reader.asInstanceOf[HiveWarehouseDataSourceReader].pushDowns.values());
    assert(transformedSql.equals(s"$originalSql LIMIT $limit"))
  }
}
