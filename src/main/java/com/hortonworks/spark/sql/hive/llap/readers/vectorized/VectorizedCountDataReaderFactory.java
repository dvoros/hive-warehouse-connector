package com.hortonworks.spark.sql.hive.llap.readers.vectorized;

import org.apache.spark.sql.sources.v2.reader.DataReader;
import org.apache.spark.sql.sources.v2.reader.DataReaderFactory;
import org.apache.spark.sql.vectorized.ColumnarBatch;

public class VectorizedCountDataReaderFactory implements DataReaderFactory<ColumnarBatch> {
  private long numRows;

  public VectorizedCountDataReaderFactory(long numRows) {
    this.numRows = numRows;
  }

  @Override
  public DataReader<ColumnarBatch> createDataReader() {
    return new VectorizedCountDataReader(numRows);
  }
}
