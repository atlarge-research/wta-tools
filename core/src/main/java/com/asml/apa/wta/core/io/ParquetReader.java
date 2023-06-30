package com.asml.apa.wta.core.io;

import java.io.IOException;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroParquetReader;

/**
 * Reads records from a Parquet file.
 *
 * @author Atour Mousavi Gourabi
 * @since 1.0.0
 */
@Slf4j
public class ParquetReader implements AutoCloseable {

  private final org.apache.parquet.hadoop.ParquetReader<GenericRecord> reader;

  /**
   * Constructs a reader to read records from Parquet.
   *
   * @param path the {@link DiskParquetInputFile} to read from
   */
  public ParquetReader(DiskParquetInputFile path) throws IOException {
    reader = AvroParquetReader.<GenericRecord>builder(path)
        .withDataModel(GenericData.get())
        .build();
  }

  /**
   * Reads the next record.
   *
   * @return the next record as a {@link GenericRecord}
   * @throws IOException when something goes wrong when reading
   */
  public GenericRecord read() throws IOException {
    return reader.read();
  }

  /**
   * Closes the reader.
   *
   * @throws IOException when something goes wrong when reading
   */
  @Override
  public void close() throws IOException {
    reader.close();
  }
}
