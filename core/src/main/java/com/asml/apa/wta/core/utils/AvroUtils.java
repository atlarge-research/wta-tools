package com.asml.apa.wta.core.utils;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.hadoop.util.HadoopOutputFile;

/**
 * Utility class for writing Parquet files using Avro based tools.
 * @since 1.0.0
 * @author Tianchen Qu
 *
 */
public class AvroUtils implements AutoCloseable {

  private final URI path;

  private final String uri;

  private final ParquetWriter<GenericRecord> writer;

  private final ParquetReader<GenericRecord> reader;

  private final Schema avroSchema;

  private Configuration configuration;

  public AvroUtils(Schema schema, File outputFolder) throws Exception {
    avroSchema = schema;
    path = outputFolder.toURI();
    org.apache.hadoop.fs.Path hadoopPath = new org.apache.hadoop.fs.Path(path);
    uri = path.getPath();
    configuration = new Configuration();
    writer = AvroParquetWriter.<GenericRecord>builder(HadoopOutputFile.fromPath(hadoopPath, configuration))
        .withSchema(avroSchema)
        .build();
    reader = AvroParquetReader.<GenericRecord>builder(HadoopInputFile.fromPath(hadoopPath, configuration))
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
      writeRecord(record);
    }
  }

  /**
   * Reader for test.
   *
   * @return the record in parquet file
   * @throws IOException possible io exception
   * @since 1.0.0
   * @author Tianchen Qu
   */
  public GenericRecord readRecord() throws IOException {
    return reader.read();
  }

  /**
   * Write single record to disk.
   *
   * @param record record
   * @throws IOException possible io exception
   * @since 1.0.0
   * @author Tianchen Qu
   */
  private void writeRecord(GenericRecord record) throws IOException {
    writer.write(record);
  }

  /**
   * Getter.
   *
   * @return output uri
   * @since 1.0.0
   * @author Tianchen Qu
   */
  public String getOutputUri() {
    return uri;
  }

  /**
   * Getter.
   *
   * @return schema for the record to be written
   * @since 1.0.0
   * @author Tianchen Qu
   */
  public Schema getAvroSchema() {
    return avroSchema;
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
