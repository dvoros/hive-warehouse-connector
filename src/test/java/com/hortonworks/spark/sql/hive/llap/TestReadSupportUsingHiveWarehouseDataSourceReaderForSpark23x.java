package com.hortonworks.spark.sql.hive.llap;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static com.hortonworks.spark.sql.hive.llap.MockHiveWarehouseConnector.DATA_SOURCE_READER_INSTANCE_COUNT_KEY;
import static com.hortonworks.spark.sql.hive.llap.MockHiveWarehouseConnector.MockHiveWarehouseDataSourceReaderForSpark23x.FINAL_HIVE_QUERY_KEY;
import static com.hortonworks.spark.sql.hive.llap.TestSecureHS2Url.TEST_HS2_URL;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestReadSupportUsingHiveWarehouseDataSourceReaderForSpark23x extends SessionTestBase {

  private HiveWarehouseSession hive;

  @Override
  @Before
  public void setUp() {
    super.setUp();
    session.conf().set(HWConf.USE_SPARK23X_SPECIFIC_READER.getQualifiedKey(), true);
    session.conf().set(HWConf.COUNT_TASKS.getQualifiedKey(), 1);

    hive = HiveWarehouseBuilder.
        session(session).
        hs2url(TEST_HS2_URL).
        build();
    HiveWarehouseSessionImpl impl = (HiveWarehouseSessionImpl) hive;
    impl.HIVE_WAREHOUSE_CONNECTOR_INTERNAL = "com.hortonworks.spark.sql.hive.llap.MockHiveWarehouseConnector";
  }

  @Override
  @After
  public void tearDown() {
    super.tearDown();
    hive.close();
  }

  @Test
  public void testSimpleQuery() {
    hive.executeQuery("select * from t1").collect();
    String expectedRegex = "^select `col1` , `col2` , `col3` from \\(select \\* from t1\\) as q_[a-zA-Z0-9]*\\s*$";
    verifyResult(expectedRegex);
  }

  @Test
  public void testSimpleFilterPushdown() {
    hive.executeQuery("select * from t1").filter("col1 > 1 ").collect();
    String expectedRegex = "^select `col1` , `col2` , `col3` from \\(select \\* from t1\\) as q_[a-zA-Z0-9]*\\s*WHERE \\(col1 IS NOT NULL AND col1 > 1\\)\\s*";
    verifyResult(expectedRegex);
  }

  @Test
  public void messAroundAndTestReaderState() {

    // 1. there should be filters in query due to df.filter("col1 > 1 ")
    Dataset<Row> df = hive.executeQuery("select * from t1");
    df.filter("col1 > 1 ").collect();
    String expectedRegex = "^select `col1` , `col2` , `col3` from \\(select \\* from t1\\) as q_[a-zA-Z0-9]*\\s*WHERE \\(col1 IS NOT NULL AND col1 > 1\\)\\s*";
    verifyResult(expectedRegex);

    // 2. now the filter should not come as df does not have any.
    df.collect();
    expectedRegex = "^select `col1` , `col2` , `col3` from \\(select \\* from t1\\) as q_[a-zA-Z0-9]*\\s*$";
    verifyResult(expectedRegex);

    // 3. in this case spark senses the the below filter is trivial and does not call pushFilters()
    // so we should get query without filters
    df.filter("col1 = col1 or col1 is null").collect();
    verifyResult(expectedRegex);
  }


  @Test
  public void testFiltersJoinedByOR() {
    // 1. here spark pushes both the filters together
    Dataset<Row> df = hive.executeQuery("select * from t1").filter("col1 < 3");
    df.filter("col1 > 1 ").collect();
    String expectedRegex = "^select `col1` , `col2` , `col3` from \\(select \\* from t1\\) as q_[a-zA-Z0-9]*\\s*WHERE \\(col1 < 3 AND col1 IS NOT NULL AND col1 > 1\\)\\s*";
    verifyResult(expectedRegex);

    // 2. Now the reader has above set of filters(col1 < 3 AND col1 IS NOT NULL AND col1 > 1)
    // when collect() is called on df, this time spark will push filter("col1 < 3")
    // so this time both the filters should be OR-ed
    df.collect();
    expectedRegex = "^select `col1` , `col2` , `col3` from \\(select \\* from t1\\) as q_[a-zA-Z0-9]*\\s*WHERE \\(col1 < 3 AND col1 IS NOT NULL\\) OR \\(col1 < 3 AND col1 IS NOT NULL AND col1 > 1\\)\\s*";
    verifyResult(expectedRegex);
  }

  @Test
  public void testCountPushDown() {
    // 1. count() should be pushed down to hive
    Dataset<Row> df = hive.executeQuery("select * from t1");
    df.count();

    String expectedRegex = "^select count\\(\\*\\) from \\(select \\* from t1\\) as q_[a-zA-Z0-9]*\\s*";
    verifyResult(expectedRegex);
  }

  @Test
  public void testCountNotPushedIfFilterIsNeededBySpark() {
    // 1. Spark will need col1 in resultset to apply filter
    // and hence we won't get prunedSchema.length = 0 and hence we don't push count() down
    Dataset<Row> df = hive.executeQuery("select * from t1").filter("col1 < 3");
    df.count();

    String expectedRegex = "^select `col1` , `col2` , `col3` from \\(select \\* from t1\\) as q_[a-zA-Z0-9]*\\s*WHERE \\(col1 < 3 AND col1 IS NOT NULL\\)\\s*";
    verifyResult(expectedRegex);
  }

  @Test
  public void testImmediateFilterAfterCountIsNotPushedDown() {
    // 1. count() is pushed down to hive
    Dataset<Row> df = hive.executeQuery("select * from t1");
    df.count();

    String expectedRegex = "^select count\\(\\*\\) from \\(select \\* from t1\\) as q_[a-zA-Z0-9]*\\s*";
    verifyResult(expectedRegex);

    // 2. Since count() was pushed down in previous step, that means pruned schema in reader is of length = 0
    // now for current filter: col1 < 3, when pushFilters() is invoked, it cannot find col1 in schema and hence does not pushes it down
    // now after pushFilters(), pruneSchema() is invoked again and correct schema is restored so the query works fine.
    df.filter("col1 < 3").collect();
    expectedRegex = "^select `col1` , `col2` , `col3` from \\(select \\* from t1\\) as q_[a-zA-Z0-9]*\\s*WHERE \\(col1 IS NOT NULL\\)\\s*";
    verifyResult(expectedRegex);
  }

  @Test
  public void testJoinWorksWithArrayIndexOutOfBounds() {
    Dataset<Row> df = hive.executeQuery("select * from t1");
    Dataset<Row> temp = df.select("col1", "col2");
    df.join(temp, "col1").collect();
    assertEquals(session.conf().get(DATA_SOURCE_READER_INSTANCE_COUNT_KEY), "1");
  }

  private void verifyResult(String expectedQueryRegex) {
    String actualQuery = session.conf().get(FINAL_HIVE_QUERY_KEY);
    assertTrue(actualQuery.matches(expectedQueryRegex));
    assertEquals(session.conf().get(DATA_SOURCE_READER_INSTANCE_COUNT_KEY), "1");
  }

}
