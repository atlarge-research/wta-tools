package com.asml.apa.wta.core.utils;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.List;

import lombok.Getter;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.hadoop.util.HadoopOutputFile;

/**
 * Utility class for writing Parquet files using Avro based tools.
 *
 * @since 1.0.0
 * @author Tianchen Qu
 */
public class AvroUtils implements AutoCloseable {

  private final URI path;

  @Getter
  private final String uri;

  private final ParquetWriter<GenericRecord> writer;

  private final ParquetReader<GenericRecord> reader;

  @Getter
  private final Schema avroSchema;

  private Configuration configuration;

  public AvroUtils(Schema schema, File outputFolder) throws Exception {
    avroSchema = schema;
    path = outputFolder.toURI();
    Path hadoopPath = new Path(path);
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
      writer.write(record);
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
