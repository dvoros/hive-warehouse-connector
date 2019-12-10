package com.hortonworks.examples;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hive.streaming.HiveStreamingConnection;
import org.apache.hive.streaming.StreamingException;
import org.apache.hive.streaming.StrictDelimitedInputWriter;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Enumeration;

public class StandaloneHiveStreamingTest {

  public static final String STREAMING_TEST_DB = "streaming_test_db";
  public static final String STREAMING_TEST_TABLE = "streaming_test_table";

  static {
    try {
      replaceSparkHiveDriver();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static void main(String[] args) throws SQLException, StreamingException, InterruptedException {

    System.out.println("StandaloneHiveStreamingTest.main: ARGS: = " + Arrays.toString(args));

    final String jdbcUrl = args[0];
    final String metastoreUri = args[1];

    String metastoreKrbPrincipal = null;
    int numRecordsToWrite = 10000;
    int commitAfterNRows = 100;
    int delayBetweenBatchesSecs = 1;


    metastoreKrbPrincipal = args[2];
    numRecordsToWrite = Integer.parseInt(args[3]);
    commitAfterNRows = Integer.parseInt(args[4]);
    delayBetweenBatchesSecs = Integer.parseInt(args[5]);

    System.out.println("jdbcUrl = " + jdbcUrl);
    System.out.println("metastoreUri = " + metastoreUri);
    System.out.println("metastoreKrbPrincipal = " + metastoreKrbPrincipal);
    System.out.println("numRecordsToWrite = " + numRecordsToWrite);
    System.out.println("commitAfterNRows = " + commitAfterNRows);
    System.out.println("delayBetweenBatchesSecs = " + delayBetweenBatchesSecs);


    final String dropDatabase = String.format("drop database if exists %s cascade", STREAMING_TEST_DB);
    final String createDb = String.format("create database %s", STREAMING_TEST_DB);

    final String createTable = String.format("create table %s.%s(c1 int, c2 string) stored as orc  TBLPROPERTIES ('transactional'='true')",
        STREAMING_TEST_DB,
        STREAMING_TEST_TABLE);

    System.out.println("=================dropping database============");
    executeSQL(jdbcUrl, dropDatabase);
    System.out.println("=================creating database============");
    executeSQL(jdbcUrl, createDb);
    System.out.println("=================creating table============");
    executeSQL(jdbcUrl, createTable);

    final StrictDelimitedInputWriter strictDelimitedInputWriter = StrictDelimitedInputWriter.newBuilder()
        .withFieldDelimiter(',')
        .build();

    HiveConf hiveConf = new HiveConf();
    hiveConf.set(MetastoreConf.ConfVars.THRIFT_URIS.getHiveName(), metastoreUri);
    hiveConf.setVar(HiveConf.ConfVars.HIVE_CLASSLOADER_SHADE_PREFIX, "shadehive");
    hiveConf.set(MetastoreConf.ConfVars.CATALOG_DEFAULT.getHiveName(), "hive");
    hiveConf.set(MetastoreConf.ConfVars.KERBEROS_PRINCIPAL.getHiveName(), metastoreKrbPrincipal);


    System.out.println("creating streaming connection....");
    HiveStreamingConnection streamingConnection = HiveStreamingConnection.newBuilder()
        .withDatabase(STREAMING_TEST_DB)
        .withTable(STREAMING_TEST_TABLE)
        .withRecordWriter(strictDelimitedInputWriter)
        .withHiveConf(hiveConf)
        .withAgentInfo("--StandaloneHiveStreamingTest--")
        .connect();


    streamingConnection.beginTransaction();

    for (int i = 0; i < numRecordsToWrite; i++) {
      if (i > 0 && i % commitAfterNRows == 0) {
        System.out.println("======Committing after " + i + " records=====");
        streamingConnection.commitTransaction();
        Thread.sleep(delayBetweenBatchesSecs * 1000);
        streamingConnection.beginTransaction();
      }
      String record = i + "," + ("string-" + i);
      streamingConnection.write(record.getBytes());
    }

    System.out.println("=======closing connection=====");
    streamingConnection.close();

  }

  private static void executeSQL(String jdbcUrl, String query) throws SQLException {
    try (Connection con = DriverManager.getConnection(jdbcUrl, "", "")) {
      Statement stmt = con.createStatement();
      stmt.execute(query);
    }
  }


  public static void replaceSparkHiveDriver() throws Exception {
    Enumeration<Driver> drivers = DriverManager.getDrivers();
    while (drivers.hasMoreElements()) {
      Driver driver = drivers.nextElement();
      String driverName = driver.getClass().getName();
      if (driverName.endsWith("HiveDriver")) {
        DriverManager.deregisterDriver(driver);
      }
    }
    DriverManager.registerDriver((Driver) Class.forName("shadehive.org.apache.hive.jdbc.HiveDriver").newInstance());
  }


}
