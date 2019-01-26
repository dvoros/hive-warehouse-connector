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
package com.hortonworks.spark.sql.hive.llap.pushdowns;

import com.hortonworks.spark.sql.hive.llap.GlobalLimitRule;
import org.apache.spark.sql.SparkSession;
import scala.Function1;

import java.util.Collection;
import java.util.List;

public interface PushDownUtil {

  //List all the pushdown rules here in the order they should be injected
  Class[] PUSHDOWN_RULES = {GlobalLimitRule.class};

  enum PushDownType {LIMIT}

  static void injectOptimizerPushdowns(SparkSession sparkSession) {
    for (Class clazz : PUSHDOWN_RULES) {
      sparkSession.
          extensions().
          injectOptimizerRule(new OptimizerFunctionWrapper(clazz));
    }
  }

  //SQL string is simply transformed in sequence by pushDowns
  //This can be made into a more formal tree transformation approach,
  //if the complexity warrants it. Keep it simple for now.
  static String pushDown(String value, Collection<Function1<String, String>> pushDowns) {
    for (Function1<String, String> pushDown : pushDowns) {
      value = pushDown.apply(value);
    }
    return value;
  }
}
