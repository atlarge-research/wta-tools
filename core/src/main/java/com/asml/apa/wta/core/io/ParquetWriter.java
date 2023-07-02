package com.asml.apa.wta.core.io;

import com.asml.apa.wta.core.model.BaseTraceObject;
import java.io.IOException;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

/**
 * Writes records to a Parquet file.
 *
 * @author Atour Mousavi Gourabi
 * @since 1.0.0
 */
@Slf4j
public class ParquetWriter<T extends BaseTraceObject> implements AutoCloseable {

  private final org.apache.parquet.hadoop.ParquetWriter<GenericRecord> writer;
  private final ParquetSchema parquetSchema;

  /**
   * Constructs a writer to write records to Parquet.
   *
   * @param path        {@link OutputFile} to write to
   * @param schema      {@link ParquetSchema} to write
   */
  public ParquetWriter(OutputFile path, ParquetSchema schema) throws IOException {
    parquetSchema = schema;
    writer = AvroParquetWriter.<GenericRecord>builder(path.wrap())
        .withSchema(schema.getAvroSchema())
        .withCompressionCodec(CompressionCodecName.SNAPPY)
        .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
        .build();
  }

  /**
   * Writes the record.
   * Provides no guarantee that the file is directly flushed.
   *
   * @param record        record to write
   * @throws IOException  when something goes wrong when writing
   */
  public void write(T record) throws IOException {
    writer.write(record.convertToRecord(parquetSchema));
  }

  /**
   * Closes the writer.
   *
   * @throws IOException  when something goes wrong when writing
   */
  @Override
  public void close() throws IOException {
    writer.close();
  }
}
