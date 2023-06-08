package com.asml.apa.wta.core.io;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.reflect.ReflectData;
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
public class ParquetWriter<T> implements AutoCloseable {

  private final org.apache.parquet.hadoop.ParquetWriter<GenericData.Record> writer;
  private final Schema avroSchema;

  /**
   * Constructs a writer to write records to Parquet.
   *
   * @param path the {@link OutputFile} to write to
   * @author Atour Mousavi Gourabi
   * @since 1.0.0
   */
  public ParquetWriter(OutputFile path, Class<T> clazz) throws IOException {
    avroSchema = ReflectData.get().getSchema(clazz);
    writer = AvroParquetWriter.<GenericData.Record>builder(path.wrap())
        .withSchema(avroSchema)
        .withCompressionCodec(CompressionCodecName.SNAPPY)
        .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
        .build();
  }

  /**
   * Writes the record.
   * Provides no guarantee that the file is directly flushed.
   *
   * @param record the record to write
   * @throws IOException when something goes wrong when writing
   * @author Atour Mousavi Gourabi
   * @since 1.0.0
   */
  public void write(T record) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);

    DatumWriter datumWriter = ReflectData.get().createDatumWriter(avroSchema);
    datumWriter.write(record, encoder);
    encoder.flush();

    GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<>(avroSchema);
    Decoder decoder = DecoderFactory.get().binaryDecoder(out.toByteArray(), null);

    writer.write((GenericData.Record) datumReader.read(null, decoder));
  }

  @Override
  public void close() throws IOException {
    writer.close();
  }
}
