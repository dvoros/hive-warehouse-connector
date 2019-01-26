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
package com.hortonworks.spark.sql.hive.llap;

import com.hortonworks.spark.sql.hive.llap.pushdowns.PushDownUtil.PushDownType.LIMIT
import com.hortonworks.spark.sql.hive.llap.util.HiveQlUtil

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.types.IntegerType
import org.slf4j.LoggerFactory

case class GlobalLimitRule(spark: SparkSession) extends Rule[LogicalPlan] {

  val LOG = LoggerFactory.getLogger(classOf[HiveWarehouseDataSourceReader])

  // Pushes an implicit DataFrame limit into LLAP
  // e.g. hive.executeQuery("select * from foo").show(100) pushes down LIMIT 100
  override def apply(plan: LogicalPlan): LogicalPlan = plan match {
    case GlobalLimit(
      Literal(value, IntegerType),
      LocalLimit(
        _,
        project@Project(
          _,
          dsr@DataSourceV2Relation(fullOutput, reader)
        )
      )
    ) => {
      if (reader.isInstanceOf[HiveWarehouseDataSourceReader]) {
        val hiveReader = reader.asInstanceOf[HiveWarehouseDataSourceReader]
        val limit = value.asInstanceOf[Int]
        LOG.info("Found limit clause, adding limit = {} to pushdowns list", limit)
        hiveReader.addPushDown(LIMIT,
          (str: String) => HiveQlUtil.withLimit(str, limit)
        )
        project
      } else {
        plan
      }
    }
    case _ => plan
  }
}
