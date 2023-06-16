package com.asml.apa.wta.core.io;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.asml.apa.wta.core.model.BaseTraceObject;
import java.io.IOException;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class ParquetWriterTest {
  @Test
  void testWriteMethodCalled() throws IOException {
    org.apache.parquet.hadoop.ParquetWriter<GenericRecord> writerMock =
        mock(org.apache.parquet.hadoop.ParquetWriter.class);

    ParquetSchema schemaMock = mock(ParquetSchema.class);

    ParquetWriter<BaseTraceObject> sut = new ParquetWriter<>(schemaMock, writerMock);

    BaseTraceObject recordMock = mock(BaseTraceObject.class);

    sut.write(recordMock);
    sut.close();

    verify(writerMock, times(1)).write(Mockito.any());
    verify(writerMock, times(1)).close();
  }
}
