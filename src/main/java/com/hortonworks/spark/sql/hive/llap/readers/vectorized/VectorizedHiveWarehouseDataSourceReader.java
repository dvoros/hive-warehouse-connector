package com.hortonworks.spark.sql.hive.llap.readers.vectorized;

import com.hortonworks.spark.sql.hive.llap.common.HWConf;
import com.hortonworks.spark.sql.hive.llap.common.CommonBroadcastInfo;
import com.hortonworks.spark.sql.hive.llap.readers.scalar.HiveWarehouseDataSourceReader;
import com.hortonworks.spark.sql.hive.llap.util.JobUtil;
import com.hortonworks.spark.sql.hive.llap.util.SchemaUtil;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.spark.sql.sources.v2.reader.DataReaderFactory;
import org.apache.spark.sql.sources.v2.reader.SupportsScanColumnarBatch;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 1. Spark pulls the unpruned schema -> readSchema()
 * 2. Spark pulls factories, where factory/task are 1:1 -> createBatchDataReaderFactories(..)
 */
public class VectorizedHiveWarehouseDataSourceReader
        extends HiveWarehouseDataSourceReader implements SupportsScanColumnarBatch {

  private static Logger LOG = LoggerFactory.getLogger(VectorizedHiveWarehouseDataSourceReader.class);

  public VectorizedHiveWarehouseDataSourceReader(Map<String, String> options) {
    super(options);
  }

  @Override
  public List<DataReaderFactory<ColumnarBatch>> createBatchDataReaderFactories() {
    try {
      boolean countStar = this.schema.length() == 0;
      String queryString = getQueryString(SchemaUtil.columnNames(schema), getPushedFilters());
      List<DataReaderFactory<ColumnarBatch>> factories = new ArrayList<>();
      if (countStar) {
        LOG.info("Executing count with query: {}", queryString);
        factories.addAll(getCountStarFactories(queryString));
      } else {
        factories.addAll(getSplitsFactories(queryString));
      }
      return factories;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private List<DataReaderFactory<ColumnarBatch>> getSplitsFactories(String query) throws IOException {
    List<DataReaderFactory<ColumnarBatch>> tasks = new ArrayList<>();
    InputSplit[] splits = super.getSplits(query);
    if (splits.length > 2) {
      LOG.info("Serializing {} actual splits to send to executors", (splits.length - 2));
      byte[] serializedJobConf = JobUtil.serializeJobConf(jobConf);
      long arrowAllocatorMax = getArrowAllocatorMax();

      long start = System.currentTimeMillis();
      for (int i = 2; i < splits.length; i++) {
        tasks.add(getDataReaderFactory(splits[i], serializedJobConf, arrowAllocatorMax, commonBroadcastInfo));
      }
      long end = System.currentTimeMillis();
      LOG.info("Serialized {} actual splits in {} millis", (splits.length - 2), (end - start));
    }
    return tasks;
  }

  private DataReaderFactory<ColumnarBatch> getDataReaderFactory(InputSplit split,
                                                                byte[] serializedJobConf,
                                                                long arrowAllocatorMax,
                                                                CommonBroadcastInfo commonBroadcastInfo) {
    return new VectorizedHiveWarehouseDataReaderFactory(split, serializedJobConf, arrowAllocatorMax, commonBroadcastInfo);
  }

  private List<DataReaderFactory<ColumnarBatch>> getCountStarFactories(String query) {
    List<DataReaderFactory<ColumnarBatch>> tasks = new ArrayList<>(100);
    long count = getCount(query);
    String numTasksString = HWConf.COUNT_TASKS.getFromOptionsMap(options);
    int numTasks = Integer.parseInt(numTasksString);
    long numPerTask = count / (numTasks - 1);
    long numLastTask = count % (numTasks - 1);
    for (int i = 0; i < (numTasks - 1); i++) {
      tasks.add(new VectorizedCountDataReaderFactory(numPerTask));
    }
    tasks.add(new VectorizedCountDataReaderFactory(numLastTask));
    return tasks;
  }
}
