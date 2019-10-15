package com.hortonworks.hwc.example;

import com.hortonworks.hwc.HiveWarehouseSession;
import com.hortonworks.spark.sql.hive.llap.HiveWarehouseSessionImpl;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;

/**
 * To test HWC java APIs with spark-submit.
 * <p>
 * To run:
 * spark-submit --class com.hortonworks.hwc.example.HWCTestRunner --master local --deploy-mode client <path_to_hive-warehouse-connector-assembly-*.jar>
 * <p>
 * This assumes that all the configurations required for HWC are set in spark-defaults.conf since spark-submit uses spark-defaults.conf by default.
 *
 * Can also be invoked with confs and args like:
 * bin/spark-submit --class com.hortonworks.hwc.example.HWCTestRunner --master local --deploy-mode client --conf spark.datasource.hive.warehouse.load.staging.dir=hdfs://localhost:50374/tmp --conf spark.hadoop.hive.zookeeper.quorum=localhost:55173 --conf spark.datasource.hive.warehouse.disable.pruning.and.pushdown=false --conf spark.sql.hive.conf.list="hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;hive.support.concurrency=true;hive.enforce.bucketing=true;hive.exec.dynamic.partition.mode=nonstrict;hive.vectorized.execution.filesink.arrow.native.enabled=true;hive.vectorized.execution.enabled=true"  /Users/schaurasia/Documents/repos/hortonworks/hive-warehouse-connector/target/scala-2.11/hive-warehouse-connector-assembly-1.0.0-SNAPSHOT.jar "select * from mini_llap_db.movie"
 *
 */
public class HWCTestRunner {

  public static void main(String[] args) {
    SparkSession spark = SparkSession
        .builder()
        .appName(HWCTestRunner.class.getName())
        .getOrCreate();

    // let it take properties from spark-defaults.conf
    HiveWarehouseSessionImpl session = HiveWarehouseSession.session(spark).build();
    System.out.println("========Showing databases========");
    session.showDatabases().show(false);
    System.out.println("========Showing databases========");

    System.out.println("EXECUTEQUERY() queries: " + Arrays.toString(args));

    if (args.length > 0) {
      for (String arg : args) {
        System.out.println("executeQuery(): " + arg);
        session.executeQuery(arg).show(1000, false);
      }
    }

    // more test code here....

    spark.stop();
  }

}