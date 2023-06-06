package com.asml.apa.wta.core.utils;

import java.io.File;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.util.HadoopOutputFile;

/**
 * Utility class for writing Parquet files using Avro based tools.
 *
 * @since 1.0.0
 * @author Tianchen Qu
 */
public class AvroUtils implements AutoCloseable {

  private final ParquetWriter<GenericRecord> writer;

  public AvroUtils(Schema schema, File outputFolder) throws Exception {
    Path hadoopPath = new Path(outputFolder.toURI());
    writer = AvroParquetWriter.<GenericRecord>builder(HadoopOutputFile.fromPath(hadoopPath, new Configuration()))
        .withSchema(schema)
        .build();
  }

  /**
   * Write batches into the disk.
   *
   * @param records list of records
   * @throws Exception possible io exception
   * @since 1.0.0
   * @author Tianchen Qu
   */
  public void writeRecords(List<GenericRecord> records) throws Exception {
    for (GenericRecord record : records) {
      writer.write(record);
    }
  }

  /**
   * Closes the writer.
   *
   * @throws Exception possible io exception
   * @since 1.0.0
   * @author Tianchen Qu
   */
  @Override
  public void close() throws Exception {
    writer.close();
  }
}
