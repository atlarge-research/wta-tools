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
    /*
    writer = AvroParquetWriter
    .<GenericRecord>builder(new org.apache.hadoop.fs.Path(path))
    .withSchema(avroSchema)
    .build();
    */
    configuration = new Configuration();
    writer = AvroParquetWriter.<GenericRecord>builder(HadoopOutputFile.fromPath(hadoopPath, configuration))
        .withSchema(avroSchema)
        .build();
    reader = AvroParquetReader.<GenericRecord>builder(HadoopInputFile.fromPath(hadoopPath, configuration))
        .build();
  }

  /**write batches into the disk.
   *
   * @param records list of records
   * @throws Exception possible io exception
   */
  public void writeRecords(List<GenericRecord> records) throws Exception {
    for (GenericRecord record : records) {
      writeRecord(record);
    }
  }

  public GenericRecord readRecord() throws IOException {
    return reader.read();
  }

  /**write single record to disk.
   *
   * @param record record
   * @throws Exception possible io exception
   */
  private void writeRecord(GenericRecord record) throws Exception {
    writer.write(record);
  }

  /**getter.
   *
   * @return output uri
   */
  public String getOutputUri() {
    return uri;
  }

  /**getter.
   *
   * @return schema for the record to be written
   */
  public Schema getAvroSchema() {
    return avroSchema;
  }

  /**closes the writer.
   *
   * @throws Exception possible io exception
   */
  @Override
  public void close() throws Exception {
    writer.close();
  }
}
